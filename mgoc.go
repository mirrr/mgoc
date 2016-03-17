package mgoc

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mirrr/types.v1"
	"reflect"
	"sync"
	"time"
)

type (
	obj map[string]interface{}
	arr []interface{}

	Cache struct {
		list       obj
		interval   time.Duration
		collection *mgo.Collection
		sliceType  reflect.Type
		el         interface{}
		key        string
		pass       bool
		query      interface{}
		sync.Mutex
	}

	CacheGroup struct {
		Cache
	}
)

func New(collection *mgo.Collection, el interface{}, key string) *Cache {
	return &Cache{
		collection: collection,
		sliceType:  reflect.SliceOf(reflect.TypeOf(el)),
		key:        key,
		el:         el,
		query:      obj{},
		interval:   time.Second,
	}
}

func (c *Cache) Query(query interface{}) *Cache {
	c.query = query
	return c
}

func (c *Cache) Interval(duration time.Duration) *Cache {
	c.interval = duration
	return c
}

func (c *Cache) Run() *Cache {
	c.Update()
	go func() {
		for range time.Tick(c.interval) {
			c.Update()
		}
	}()
	return c
}

func (c *Cache) Update() {
	if !c.pass {
		c.pass = true
		defer func() { c.pass = false }()

		ptr := reflect.New(c.sliceType)
		ptr.Elem().Set(reflect.MakeSlice(c.sliceType, 0, 0))
		list := ptr.Interface()

		if err := c.collection.Find(c.query).All(list); err != nil {
			return
		}

		c.Lock()
		defer c.Unlock()
		c.list = obj{}
		listV := reflect.ValueOf(list).Elem()
		for i := 0; i < listV.Len(); i++ {
			current := listV.Index(i)
			id := types.String(reflect.Indirect(current).FieldByName(c.key))
			c.list[id] = current.Interface()
		}
	}
}

func (c *Cache) Get(key interface{}) interface{} {
	c.Lock()
	defer c.Unlock()
	return c.list[types.String(key)]
}
