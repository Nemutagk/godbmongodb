package godbmongodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Nemutagk/godb/v2"
	"github.com/Nemutagk/godb/v2/definitions/models"
	"github.com/Nemutagk/godb/v2/definitions/repository"
	"github.com/Nemutagk/golog"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Model interface {
	ScanFields() []any
}

type NewConnectionConfig struct {
	Name             string
	Collection       string
	OrderColumns     map[string]string
	InsertId         *bool
	InsertTimestamps *bool
	SoftDelete       *string
	Relationer       map[string]repository.RelationLoader
}

type OnetoManyLoader[P Model, C Model] struct {
	Repository     repository.DriverConnection[C]
	ParentField    string
	ChildFkField   string
	ContainerField string
}

func (o *OnetoManyLoader[P, C]) Load(ctx context.Context, parentModels []any, childs *[]string) error {
	var parentIds []any

	for _, pModel := range parentModels {
		val := reflect.ValueOf(pModel)

		for val.Kind() == reflect.Ptr {
			val = val.Elem()
		}

		parentFieldVal := val.FieldByName(o.ParentField)
		if !parentFieldVal.IsValid() {
			return fmt.Errorf("parent field '%s' not found in model", o.ParentField)
		}

		parentId := parentFieldVal.Interface()
		parentIds = append(parentIds, parentId)
	}

	in := models.ComparatorIn
	filter := models.GroupFilter{
		Filters: []any{
			models.FilterMultipleValue{
				Key:        o.ChildFkField,
				Values:     parentIds,
				Comparator: &in,
			},
		},
	}

	opts := models.Options{}

	if childs != nil && len(*childs) > 0 {
		opts.Relations = *childs
	}

	foundChilds, err := o.Repository.Get(ctx, filter, &opts)
	if err != nil {
		return err
	}

	for _, pChild := range foundChilds {
		valForFieldAccess := reflect.ValueOf(pChild)
		for valForFieldAccess.Kind() == reflect.Ptr {
			valForFieldAccess = valForFieldAccess.Elem()
		}

		foreignKeyTmp := prepareForeignKey(o.ChildFkField)
		childFkValue := valForFieldAccess.FieldByName(foreignKeyTmp)
		if !childFkValue.IsValid() {
			return fmt.Errorf("child foreign key field '%s' not found in model", foreignKeyTmp)
		}

		foreignKey := childFkValue.Interface()

		for _, parent := range parentModels {
			parentVal := reflect.ValueOf(parent)
			for parentVal.Kind() == reflect.Ptr {
				parentVal = parentVal.Elem()
			}

			parentIdField := parentVal.FieldByName(o.ParentField)
			if !parentIdField.IsValid() {
				return fmt.Errorf("invalid parent field: %s", o.ParentField)
			}
			parentId := parentIdField.Interface()

			if foreignKey != parentId {
				continue
			}

			containerField := parentVal.FieldByName(o.ContainerField)
			if !containerField.IsValid() {
				return fmt.Errorf("invalid container field: %s", o.ContainerField)
			}

			elemToAppend := valForFieldAccess
			if containerField.Type().Elem().Kind() != reflect.Ptr && elemToAppend.Kind() == reflect.Ptr {
				elemToAppend = elemToAppend.Elem()
			} else if containerField.Type().Elem().Kind() == reflect.Ptr && elemToAppend.Kind() != reflect.Ptr {
				ptr := reflect.New(elemToAppend.Type())
				ptr.Elem().Set(elemToAppend)
				elemToAppend = ptr
			}

			switch containerField.Kind() {
			case reflect.Slice:
				containerField.Set(reflect.Append(containerField, elemToAppend))
			case reflect.Ptr:
				if containerField.Type().Elem().Kind() != reflect.Slice {
					return fmt.Errorf("container field pointer is not pointing to a slice: %s", o.ContainerField)
				}

				if containerField.IsNil() {
					sliceType := containerField.Type().Elem()
					emptySlce := reflect.MakeSlice(sliceType, 0, 0)
					ptr := reflect.New(sliceType)
					ptr.Elem().Set(emptySlce)
					containerField.Set(ptr)
				}

				sliceVal := containerField.Elem()
				sliceVal = reflect.Append(sliceVal, elemToAppend)
				containerField.Elem().Set(sliceVal)
			default:
				return fmt.Errorf("container field is not a slice or pointer to slice: %s", o.ContainerField)
			}
		}
	}

	return nil
}

type ManyToManyLoader[P Model, C Model] struct {
	Repository      repository.DriverConnection[C]
	Connection      any
	ParentKey       string
	ChildKey        string
	PivoteParentKey string
	PivoteChildKey  string
	PivoteTable     string
	ContainerField  string
}

func (m *ManyToManyLoader[P, C]) Load(ctx context.Context, parentModels []any, childs *[]string) error {
	if len(parentModels) == 0 {
		return nil
	}

	parentModelsIds := []any{}
	for _, model := range parentModels {
		val := reflect.ValueOf(model)
		for val.Kind() == reflect.Ptr {
			val = val.Elem()
		}

		parentIdField := val.FieldByName(m.ParentKey)
		if !parentIdField.IsValid() {
			return fmt.Errorf("invalid parent key field: %s", m.ParentKey)
		}

		parentModelsIds = append(parentModelsIds, parentIdField.Interface())
	}

	dbConn, ok := m.Connection.(*mongo.Database)
	if !ok {
		return fmt.Errorf("failed to assert type to *mongo.Database")
	}

	coll := dbConn.Collection(m.PivoteTable)

	filters := bson.M{
		m.PivoteParentKey: bson.M{"$in": parentModelsIds},
	}

	opts := options.Find().SetProjection(bson.D{
		{Key: m.PivoteParentKey, Value: 1},
		{Key: m.PivoteChildKey, Value: 1},
	})

	cursor, err := coll.Find(ctx, filters, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var result []map[string]any

	if err := cursor.All(ctx, &result); err != nil {
		return err
	}

	listChildForParent := map[any][]any{}
	listAllChildIds := []any{}
	for _, row := range result {
		parentId := row[m.PivoteParentKey]
		childId := row[m.PivoteChildKey]

		if _, exist := listChildForParent[childId]; !exist {
			listChildForParent[parentId] = []any{}
		}

		listChildForParent[parentId] = append(listChildForParent[parentId], childId)
		listAllChildIds = append(listAllChildIds, childId)
	}

	finalContainerChilds := map[string][]any{}
	if len(listAllChildIds) > 0 {
		in := models.ComparatorIn
		filters := models.GroupFilter{
			Filters: []any{
				models.FilterMultipleValue{
					Key:        m.ChildKey,
					Values:     listAllChildIds,
					Comparator: &in,
				},
			},
		}

		otps := models.Options{}

		if childs != nil && len(*childs) > 0 {
			otps.Relations = *childs
		}

		allChildren, err := m.Repository.Get(ctx, filters, &otps)
		if err != nil {
			return err
		}

		for _, child := range allChildren {
			valForFieldAcces := reflect.ValueOf(child)
			for valForFieldAcces.Kind() == reflect.Ptr {
				valForFieldAcces = valForFieldAcces.Elem()
			}

			childKeyField := valForFieldAcces.FieldByName(m.ChildKey)
			if !childKeyField.IsValid() {
				return fmt.Errorf("invalid child key field: %s", m.ChildKey)
			}

			childKeyValue := childKeyField.Interface()

			parentIdsForChild, exists := listChildForParent[childKeyValue]
			if !exists {
				golog.Log(ctx, "No parent IDs found for child key value:", childKeyValue)
				continue
			}

			for _, parentId := range parentIdsForChild {
				_, exists := finalContainerChilds[fmt.Sprintf("%v", parentId)]
				if !exists {
					finalContainerChilds[fmt.Sprintf("%v", parentId)] = []any{}
				}

				elemToAppend := valForFieldAcces
				if reflect.TypeOf(finalContainerChilds[fmt.Sprintf("%v", parentId)]).Elem().Kind() != reflect.Ptr && elemToAppend.Kind() == reflect.Ptr {
					elemToAppend = elemToAppend.Elem()
				} else if reflect.TypeOf(finalContainerChilds[fmt.Sprintf("%v", parentId)]).Elem().Kind() == reflect.Ptr && elemToAppend.Kind() != reflect.Ptr {
					ptr := reflect.New(elemToAppend.Type())
					ptr.Elem().Set(elemToAppend)
					elemToAppend = ptr
				}

				finalContainerChilds[fmt.Sprintf("%v", parentId)] = append(finalContainerChilds[fmt.Sprintf("%v", parentId)], elemToAppend.Interface())
			}
		}
	} else {
		golog.Log(ctx, "no child IDs Found in pivot collection for parent IDs: %v", parentModels)
	}

	for _, parentModel := range parentModels {
		val := reflect.ValueOf(parentModel)
		for val.Kind() == reflect.Ptr {
			val = val.Elem()
		}

		var parentPtr reflect.Value
		if val.Kind() == reflect.Ptr {
			parentPtr = val
		} else if val.Kind() == reflect.Struct {
			if !val.CanAddr() {
				return fmt.Errorf("cannot get address of parent model")
			}
			parentPtr = val.Addr()
		} else {
			return fmt.Errorf("invalid parent model type")
		}

		parentVal := parentPtr
		for parentVal.Kind() == reflect.Ptr {
			parentVal = parentVal.Elem()
		}

		if parentVal.Kind() != reflect.Struct {
			return fmt.Errorf("invalid parent model kind, expected struct but got %s", parentVal.Kind().String())
		}

		parentIdField := parentVal.FieldByName(m.ParentKey)
		if !parentIdField.IsValid() {
			return fmt.Errorf("invalid parent key field: %s", m.ParentKey)
		}
		parentKeyValue := fmt.Sprintf("%v", parentIdField.Interface())

		childs, ok := finalContainerChilds[parentKeyValue]
		if !ok {
			continue
		}

		containerField := parentVal.FieldByName(m.ContainerField)
		if !containerField.IsValid() {
			return fmt.Errorf("invalid container field: %s", m.ContainerField)
		}

		var sliceType reflect.Type

		switch containerField.Kind() {
		case reflect.Slice:
			sliceType = containerField.Type()
		case reflect.Ptr:
			if containerField.Type().Elem().Kind() != reflect.Slice {
				return fmt.Errorf("container field pointer is not pointing to a slice: %s", m.ContainerField)
			}

			if containerField.IsNil() {
				sliceType = containerField.Type().Elem()
				emptySlice := reflect.MakeSlice(sliceType, 0, len(childs))
				ptr := reflect.New(sliceType)
				ptr.Elem().Set(emptySlice)
				containerField.Set(ptr)
			}

			sliceType = containerField.Elem().Type()

		default:
			return fmt.Errorf("container field is not a slice or pointer to slice: %s", m.ContainerField)
		}

		elemType := sliceType.Elem()

		newSlice := reflect.MakeSlice(sliceType, 0, len(childs))

		for _, c := range childs {
			cv := reflect.ValueOf(c)

			if elemType.Kind() == reflect.Ptr {
				if cv.Kind() != reflect.Ptr {
					if cv.Type() != elemType.Elem() {
						return fmt.Errorf("child type %s does not match container element type %s",
							cv.Type(), elemType)
					}
					ptr := reflect.New(cv.Type())
					ptr.Elem().Set(cv)
					cv = ptr
				} else {
					if cv.Type() != elemType {
						return fmt.Errorf("child pointer type %s does not match container element type %s",
							cv.Type(), elemType)
					}
				}
			} else {
				if cv.Kind() == reflect.Ptr {
					cv = cv.Elem()
				}
				if cv.Type() != elemType {
					return fmt.Errorf("child value type %s does not match container element type %s",
						cv.Type(), elemType)
				}
			}

			newSlice = reflect.Append(newSlice, cv)
		}

		switch containerField.Kind() {
		case reflect.Slice:
			containerField.Set(newSlice) // []T = []T
		case reflect.Ptr:
			containerField.Elem().Set(newSlice) // *([]T) -> []T
		}
	}

	return nil
}

type OnetoOneLoader[P Model, C Model] struct {
	Repository     repository.DriverConnection[C]
	ParentField    string
	ChildFkField   string
	ContainerField string
}

func (c *OnetoOneLoader[P, C]) Load(ctx context.Context, parentModel []any, childs *[]string) error {
	if parentModel == nil || len(parentModel) == 0 {
		return nil
	}

	val := reflect.ValueOf(parentModel[0])

	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	parentIdField := val.FieldByName(c.ParentField)
	if !parentIdField.IsValid() {
		return fmt.Errorf("invalid parent field: %s", c.ParentField)
	}
	parentId := parentIdField.Interface()

	filterChild := models.GroupFilter{
		Filters: []any{
			models.Filter{Key: c.ChildFkField, Value: parentId},
		},
	}

	opts := models.Options{}

	if childs != nil && len(*childs) > 0 {
		opts.Relations = *childs
	}

	childModel, err := c.Repository.GetOne(ctx, filterChild, &opts)
	if err != nil {
		return fmt.Errorf("failed to load child model: %w", err)
	}

	containerFlied := val.FieldByName(c.ContainerField)
	if !containerFlied.IsValid() {
		return fmt.Errorf("invalid container field: %s", c.ContainerField)
	}

	if !containerFlied.CanSet() {
		return fmt.Errorf("cannot set container field: %s", c.ContainerField)
	}

	childVal := reflect.ValueOf(childModel)

	switch containerFlied.Kind() {
	case reflect.Ptr:
		if childVal.Kind() != reflect.Ptr {
			if childVal.Type() != containerFlied.Type().Elem() {
				return fmt.Errorf("child type %s does not match container field type %s",
					childVal.Type(), containerFlied.Type().Elem())
			}
			ptr := reflect.New(childVal.Type())
			ptr.Elem().Set(childVal)
			childVal = ptr
		} else {
			if childVal.Type() != containerFlied.Type() {
				return fmt.Errorf("child pointer type %s does not match container field type %s",
					childVal.Type(), containerFlied.Type())
			}
		}
		containerFlied.Set(childVal)
	case reflect.Interface:
		if childVal.Type().Implements(containerFlied.Type()) {
			containerFlied.Set(childVal)
		} else {
			return fmt.Errorf("child type %s does not implement container interface type %s",
				childVal.Type(), containerFlied.Type())
		}
	default:
		if childVal.Kind() == reflect.Ptr {
			childVal = childVal.Elem()
		}
		if childVal.Type() != containerFlied.Type() {
			return fmt.Errorf("child value type %s does not match container field type %s",
				childVal.Type(), containerFlied.Type())
		}
		containerFlied.Set(childVal)
	}

	return nil
}

type Connection[T Model] struct {
	Name             string
	Collection       string
	RawConnection    *mongo.Database
	Connection       *mongo.Collection
	OrderColumns     map[string]string
	SoftDelete       *string
	RelationLoaders  map[string]repository.RelationLoader
	InsertId         bool
	InsertTimestamps bool
}

func NewConnection[T Model](config NewConnectionConfig) (repository.DriverConnection[T], error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dbConn, err := godb.GetConnection(config.Name)
	if err != nil {
		return nil, err
	}

	dbConnDatabase, ok := dbConn.Connection.Adapter.GetConnection().(*mongo.Database)
	if !ok {
		golog.Error(ctx, "failed to assert type to *mongo.Client")
		return nil, godb.ErrConnectionNotFound
	}

	dbCollection := dbConnDatabase.Collection(config.Collection)

	return &Connection[T]{
		Name:             config.Name,
		Collection:       config.Collection,
		RawConnection:    dbConnDatabase,
		Connection:       dbCollection,
		OrderColumns:     config.OrderColumns,
		SoftDelete:       config.SoftDelete,
		RelationLoaders:  config.Relationer,
		InsertId:         config.InsertId != nil && *config.InsertId,
		InsertTimestamps: config.InsertTimestamps != nil && *config.InsertTimestamps,
	}, nil
}

func (c *Connection[T]) GetTableName() string {
	return c.Collection
}

func (c *Connection[T]) GetConnectionName() string {
	return c.Name
}

func (c *Connection[T]) GetOrderColumns() map[string]string {
	return c.OrderColumns
}

func (c *Connection[T]) GetConnection() any {
	return c.RawConnection
}

func (c *Connection[T]) AddRelation(name string, loader repository.RelationLoader) error {
	if c.RelationLoaders == nil {
		c.RelationLoaders = make(map[string]repository.RelationLoader)
	}

	if _, exists := c.RelationLoaders[name]; exists {
		return errors.New(fmt.Sprintf("the relation '%s' are exists in relations list", name))
	}

	c.RelationLoaders[name] = loader
	return nil
}

func (c *Connection[T]) Get(ctx context.Context, filters models.GroupFilter, opts *models.Options) ([]T, error) {
	var results []T

	if c.SoftDelete != nil {
		isNull := models.ComparatorIsNull
		deletedFilter := models.Filter{
			Key:        *c.SoftDelete,
			Comparator: &isNull,
		}
		filters.Filters = append(filters.Filters, deletedFilter)
	}

	bsonFilters := prepareFilters(filters)

	var cursor *mongo.Cursor
	var err error
	if opts != nil {
		mOpts := prepareOpts(opts)

		cursor, err = c.Connection.Find(ctx, bsonFilters, mOpts)
	} else {
		cursor, err = c.Connection.Find(ctx, bsonFilters)
	}

	if err != nil {
		return nil, err
	}

	defer cursor.Close(ctx)

	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	if opts != nil && len(opts.Relations) > 0 && c.RelationLoaders != nil {
		modelPointers := make([]*T, len(results))
		for i := range results {
			modelPointers[i] = &results[i]
		}

		anyModels := make([]any, len(modelPointers))
		for i, m := range modelPointers {
			anyModels[i] = m
		}

		for _, relation := range opts.Relations {
			childs := &[]string{}
			if strings.Contains(relation, ".") {
				tmp_items := strings.Split(relation, ".")
				relation = tmp_items[0]
				next_childs := strings.Join(tmp_items[1:], ".")
				*childs = []string{next_childs}
			}

			loader, ok := c.RelationLoaders[relation]
			if !ok {
				return nil, fmt.Errorf("lreation loader not found for relation: %s", relation)
			}

			if err := loader.Load(ctx, anyModels, childs); err != nil {
				return nil, fmt.Errorf("falied to load relation %s: %s", relation, err)
			}
		}
	}

	return results, nil
}

func (c *Connection[T]) GetOne(ctx context.Context, filters models.GroupFilter, opts *models.Options) (T, error) {
	if opts == nil {
		opts = &models.Options{
			Limit: 1,
		}
	} else {
		opts.Limit = 1
	}

	mFilters := prepareFilters(filters)

	if opts != nil {
		mOpts := options.FindOne()

		if opts.OrderColumn != "" {
			orderDirection := 1
			if opts.OrderDir != "" {
				if opts.OrderDir == "asc" {
					orderDirection = 1
				} else {
					orderDirection = -1
				}
			}
			mOpts.SetSort(bson.D{{Key: opts.OrderColumn, Value: orderDirection}})
		}

		var result T
		err := c.Connection.FindOne(ctx, mFilters, mOpts).Decode(&result)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return *new(T), nil
			}
			return *new(T), err
		}

		return result, nil
	}

	var result T
	err := c.Connection.FindOne(ctx, mFilters).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return *new(T), nil
		}
		return *new(T), err
	}

	if opts != nil && len(opts.Relations) > 0 && c.RelationLoaders != nil {
		modelPointers := &result

		anyModels := []any{modelPointers}

		for _, relation := range opts.Relations {
			childs := &[]string{}
			if strings.Contains(relation, ".") {
				tmp_items := strings.Split(relation, ".")
				relation = tmp_items[0]
				next_childs := strings.Join(tmp_items[1:], ".")
				*childs = []string{next_childs}
			}

			loader, ok := c.RelationLoaders[relation]
			if !ok {
				return *new(T), fmt.Errorf("lreation loader not found for relation: %s", relation)
			}

			if err := loader.Load(ctx, anyModels, childs); err != nil {
				return *new(T), fmt.Errorf("falied to load relation %s: %s", relation, err)
			}
		}
	}

	return result, nil
}

func (c *Connection[T]) Create(ctx context.Context, data map[string]any, opts *models.Options) (T, error) {
	var payload bson.M

	if c.InsertId {
		id := "_id"

		if opts != nil && *opts.PrimaryKey != "" {
			id = *opts.PrimaryKey
		}

		uuid, err := uuid.NewV7()
		if err != nil {
			return *new(T), err
		}
		payload = bson.M{
			id: uuid.String(),
		}
	}

	for key, value := range data {
		payload[key] = value
	}

	if opts != nil && c.InsertTimestamps {
		now := time.Now().UTC()
		payload["created_at"] = now
		payload["updated_at"] = now
	}

	res, err := c.Connection.InsertOne(ctx, payload)
	if err != nil {
		return *new(T), err
	}

	filter := models.GroupFilter{
		Filters: []any{
			models.Filter{
				Key:   "_id",
				Value: res.InsertedID,
			},
		},
	}

	found, err := c.GetOne(ctx, filter, nil)
	if err != nil {
		return *new(T), err
	}

	return found, nil
}

func (c *Connection[T]) CreateMany(ctx context.Context, data []map[string]any, opts *models.Options) ([]T, error) {
	var payloads []any

	for _, item := range data {
		payload := bson.M{}

		if c.InsertId {
			id := "_id"

			if opts != nil && *opts.PrimaryKey != "" {
				id = *opts.PrimaryKey
			}

			uuid, err := uuid.NewV7()
			if err != nil {
				return nil, err
			}
			payload[id] = uuid.String()
		}

		for key, value := range item {
			payload[key] = value
		}

		if opts != nil && c.InsertTimestamps {
			now := time.Now().UTC()
			payload["created_at"] = now
			payload["updated_at"] = now
		}

		payloads = append(payloads, payload)
	}

	res, err := c.Connection.InsertMany(ctx, payloads)
	if err != nil {
		return nil, err
	}

	var insertedIds []any
	for _, id := range res.InsertedIDs {
		insertedIds = append(insertedIds, id)
	}

	in := models.ComparatorIn
	filter := models.GroupFilter{
		Filters: []any{
			models.FilterMultipleValue{
				Key:        "_id",
				Values:     insertedIds,
				Comparator: &in,
			},
		},
	}

	found, err := c.Get(ctx, filter, nil)
	if err != nil {
		return nil, err
	}

	return found, nil
}

func (c *Connection[T]) Update(ctx context.Context, filters models.GroupFilter, data map[string]any, opts *models.Options) (T, error) {
	if c.SoftDelete != nil {
		isNull := models.ComparatorIsNull
		deletedFilter := models.Filter{
			Key:        *c.SoftDelete,
			Comparator: &isNull,
		}
		filters.Filters = append(filters.Filters, deletedFilter)
	}

	if opts != nil && c.InsertTimestamps {
		data["updated_at"] = time.Now().UTC()
	}

	bsonFilters := prepareFilters(filters)

	update := bson.M{
		"$set": data,
	}

	_, err := c.Connection.UpdateMany(ctx, bsonFilters, update)
	if err != nil {
		return *new(T), err
	}

	updated, err := c.GetOne(ctx, filters, nil)
	if err != nil {
		return *new(T), err
	}

	return updated, nil
}

func (c *Connection[T]) Delete(ctx context.Context, filters models.GroupFilter) error {
	if c.SoftDelete != nil {
		isNull := models.ComparatorIsNull
		deletedFilter := models.Filter{
			Key:        *c.SoftDelete,
			Comparator: &isNull,
		}
		filters.Filters = append(filters.Filters, deletedFilter)
	}

	bsonFilters := prepareFilters(filters)

	if c.SoftDelete != nil {
		update := bson.M{
			"$set": bson.M{
				*c.SoftDelete: time.Now().UTC(),
			},
		}

		_, err := c.Connection.UpdateMany(ctx, bsonFilters, update)
		if err != nil {
			return err
		}

		return nil
	}

	_, err := c.Connection.DeleteMany(ctx, bsonFilters)
	if err != nil {
		return err
	}

	return nil
}

func (c *Connection[T]) TransactionStart(ctx context.Context) (*models.Transaction, error) {
	session, err := c.RawConnection.Client().StartSession()
	if err != nil {
		return nil, err
	}

	if err := session.StartTransaction(); err != nil {
		return nil, err
	}

	return &models.Transaction{
		Tx:   session,
		Name: c.Name,
	}, nil
}

func (c *Connection[T]) TransactionCommit(ctx context.Context, tx *models.Transaction) error {
	mSession, ok := tx.Tx.(mongo.Session)
	if !ok {
		return errors.New("invalid transaction session")
	}

	if err := mSession.CommitTransaction(ctx); err != nil {
		return err
	}

	return nil
}

func (c *Connection[T]) TransactionRollback(ctx context.Context, tx *models.Transaction) error {
	mSession, ok := tx.Tx.(mongo.Session)
	if !ok {
		return errors.New("invalid transaction session")
	}

	if err := mSession.AbortTransaction(ctx); err != nil {
		return err
	}

	return nil
}

func (c *Connection[T]) Count(ctx context.Context, filters models.GroupFilter) (int64, error) {
	if c.SoftDelete != nil {
		isNull := models.ComparatorIsNull
		deletedFilter := models.Filter{
			Key:        *c.SoftDelete,
			Comparator: &isNull,
		}
		filters.Filters = append(filters.Filters, deletedFilter)
	}

	bsonFilters := prepareFilters(filters)

	count, err := c.Connection.CountDocuments(ctx, bsonFilters)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func prepareFilters(filters models.GroupFilter) bson.M {
	bsonFilters := bson.M{}

	for _, filter := range filters.Filters {
		if filterF, ok := filter.(models.Filter); ok {
			comparator := models.ComparatorEqual

			if filterF.Comparator != nil && *filterF.Comparator != "" {
				comparator = *filterF.Comparator
			}

			switch comparator {
			case models.ComparatorEqual:
				bsonFilters[filterF.Key] = filterF.Value
			case models.ComparatorNotEqual:
				bsonFilters[filterF.Key] = bson.M{"$ne": filterF.Value}
			case models.ComparatorGreaterThan:
				bsonFilters[filterF.Key] = bson.M{"$gt": filterF.Value}
			case models.ComparatorGreaterThanOrEqual:
				bsonFilters[filterF.Key] = bson.M{"$gte": filterF.Value}
			case models.ComparatorLessThan:
				bsonFilters[filterF.Key] = bson.M{"$lt": filterF.Value}
			case models.ComparatorLessThanOrEqual:
				bsonFilters[filterF.Key] = bson.M{"$lte": filterF.Value}
			case models.ComparatorLike:
				bsonFilters[filterF.Key] = bson.M{"$regex": filterF.Value}
			case models.ComparatorIsNull:
				bsonFilters[filterF.Key] = nil
			case models.ComparatorIsNotNull:
				bsonFilters[filterF.Key] = bson.M{"$ne": nil}
				// case models.ComparatorIn:
				// 	bsonFilters[filterF.Field] = bson.M{"$in": filterF.Value}
			}
		} else if filterGroup, ok := filter.(models.GroupFilter); ok {
			operator := models.OperatorAnd
			if filterF.Operator != nil && *filterF.Operator != "" {
				operator = *filterF.Operator
			}

			bsonFilters[filterF.Key] = bson.M{operator: prepareFilters(filterGroup)}
		} else if filterArr, ok := filter.(models.FilterMultipleValue); ok {
			comparator := models.ComparatorEqual

			if filterArr.Comparator != nil && *filterArr.Comparator != "" {
				comparator = *filterArr.Comparator
			}

			switch comparator {
			case models.ComparatorIn:
				bsonFilters[filterArr.Key] = bson.M{"$in": filterArr.Values}
			case models.ComparatorNotIn:
				bsonFilters[filterArr.Key] = bson.M{"$nin": filterArr.Values}
			}
		}
	}

	return bsonFilters
}

func prepareOpts(opts *models.Options) *options.FindOptions {
	mOpts := options.Find()

	if opts.Limit != 0 {
		mOpts.SetLimit(opts.Limit)
	}

	if opts.Offset != 0 {
		mOpts.SetSkip(opts.Offset)
	}

	if opts.OrderColumn != "" {
		orderDirection := 1
		if opts.OrderDir != "" {
			if opts.OrderDir == "asc" {
				orderDirection = 1
			} else {
				orderDirection = -1
			}
		}
		mOpts.SetSort(bson.D{{Key: opts.OrderColumn, Value: orderDirection}})
	}

	return mOpts
}

func prepareForeignKey(str string) string {
	// return strings.ToUpper(str[:1]) + str[1:]

	parts := strings.Split(str, "_")

	buffer := strings.Builder{}

	for i := range parts {
		// if i > 0 {
		// 	buffer.WriteString("_")
		// }

		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		buffer.WriteString(parts[i])
	}

	return buffer.String()
}
