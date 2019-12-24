package mongodb

import (
	"fmt"
	"log"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var session *mgo.Session

//Test 测试
type Test struct {
	Name string
	Age  int
	Sex  string
}

//Connect 连接
type Connect struct {
	collect *mgo.Collection
}

func init() {
	var err error
	session, err = mgo.Dial("127.0.0.1:27017") //连接
	if err != nil {
		log.Println("mongodb连接失败：", err)
	}
	session.SetMode(mgo.Monotonic, true)
}

//设置集合
func connect(db, collection string) *mgo.Collection {
	c := session.DB(db).C(collection)
	return c
}

// //检查错误
func failOnErr(msg string, err error) {

	if err != nil {
		log.Fatal(msg, err)
	}
	fmt.Println("操作成功！")
}

// //添加
func (c *Connect) add(dname, cname string) {

	//session和collection
	c.collect = connect(dname, cname)
	err := c.collect.Insert(&Test{"小文", 18, "男"})
	failOnErr("添加错误：", err)
}

// //删除
func (c *Connect) del(dname, cname string, selector interface{}) {

	//session和collection
	c.collect = connect(dname, cname)
	err := c.collect.Remove(selector)
	failOnErr("删除错误：", err)
}

// //修改
func (c *Connect) change(dname, cname string, selector, update interface{}) {

	//session和collection
	cc := connect(dname, cname)
	err := cc.Update(selector, update)
	failOnErr("修改错误：", err)
}

// //单个查询
func (c *Connect) findOne(dname, cname string, query, result interface{}) {

	//session和collection
	c.collect = connect(dname, cname)
	err := c.collect.Find(query).One(result)
	failOnErr("单行查询错误：", err)
}

// //多行查询
func (c *Connect) findAll(dname, cname string, query, selector, result interface{}) {

	//session和collection
	cc := connect(dname, cname)
	err := cc.Find(query).Select(selector).All(result)
	failOnErr("多行错误：", err)
}
