package mgoc

import (
	"fmt"
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
		elType     reflect.Type
		idField    string
		pass       bool
		runned     bool
		query      interface{}
		path       []string
		pathObj    obj
		sync.Mutex
	}

	CacheGroup struct {
		Cache
	}
)

func New(collection *mgo.Collection, el interface{}, idField string) *Cache {
	return &Cache{
		collection: collection,
		sliceType:  reflect.SliceOf(reflect.TypeOf(el)),
		idField:    idField,
		elType:     reflect.TypeOf(el),
		query:      obj{},
		interval:   time.Second,
	}
}

func (c *Cache) Query(query interface{}) *Cache {
	c.query = query
	return c
}

func (c *Cache) Group(path ...string) *Cache {
	c.path = path
	return c
}

func (c *Cache) Interval(duration time.Duration) *Cache {
	c.interval = duration
	return c
}

func (c *Cache) Run() *Cache {
	if !c.runned {
		c.runned = true
		c.Update()
		go func() {
			for range time.Tick(c.interval) {
				if !c.runned {
					return
				}
				c.Update()
			}
		}()
	}
	return c
}

func (c *Cache) Update() {
	if !c.pass {
		c.pass = true
		defer func() { c.pass = false }()

		ptr := reflect.New(c.sliceType)
		ptr.Elem().Set(reflect.MakeSlice(c.sliceType, 0, 0))
		array := ptr.Interface()

		if err := c.collection.Find(c.query).All(array); err != nil {
			fmt.Println(err)
			return
		}

		c.Lock()
		defer c.Unlock()
		c.list = obj{}
		c.pathObj = obj{}
		arrayVl := reflect.ValueOf(array).Elem()
		for i := 0; i < arrayVl.Len(); i++ {
			current := arrayVl.Index(i)
			idval := reflect.Indirect(current).FieldByName(c.idField)
			if idval.Kind() == reflect.Invalid {
				panic("Invalid ID struct field: " + c.idField)
			}
			id := types.String(idval)
			c.list[id] = current.Interface()

			// рекурсивная группировка по полям структуры
			if len(c.path) > 0 {
				c.branch(c.pathObj, current, id, 0)
			}
		}
	}
}

func (c *Cache) branch(po interface{}, current reflect.Value, id string, k int) {
	v := c.path[k]
	last := k+1 == len(c.path)
	if len(v) > 0 {
		process := func(val string) {
			if !last {
				if _, exists := po.(obj)[val]; !exists {
					po.(obj)[val] = obj{}
				}
				c.branch(po.(obj)[val], current, id, k+1)
			} else {
				if _, exists := po.(obj)[val]; !exists {
					po.(obj)[val] = arr{}
				}
				po.(obj)[val] = append(po.(obj)[val].(arr), id)
			}
		}

		fval := reflect.Indirect(current).FieldByName(v)
		if fval.Kind() == reflect.Invalid {
			panic("Invalid struct field: " + v)
		} else if fval.Kind() == reflect.Slice {
			for j := 0; j < fval.Len(); j++ {
				process(types.String(fval.Index(j)))
			}
		} else {
			process(types.String(fval))
		}
	}
}

func (c *Cache) Get(id interface{}) interface{} {
	c.Lock()
	defer c.Unlock()
	if ret, exists := c.list[types.String(id)]; exists {
		return ret
	}
	return reflect.New(c.elType).Interface()
}

func (c *Cache) Len() int {
	c.Lock()
	defer c.Unlock()
	return len(c.list)
}

func (c *Cache) GetByGroup(keys ...interface{}) (ret interface{}) {
	ret = []string{}
	c.Lock()
	defer c.Unlock()
	if len(c.pathObj) < len(keys) {
		return
	}
	ret = types.MGet(c.pathObj, keys...)
	return
}
