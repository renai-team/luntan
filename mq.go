package rabbitmq

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

const (
	//Direct 直行交换机
	Direct = "dircet"
	//Fanout 扇形交换机
	Fanout = "fanout"
	//Topic 主题交换机
	Topic = "topic"
	//Headers 首部交换机
	Headers = "headers"
)

//Connect 测试
type Connect struct {
	conn     *amqp.Connection      //连接
	ch       *amqp.Channel         //通道
	queue    map[string]amqp.Queue //队列、名字map
	exchange []Exchange            //交换机
}

//Exchange 交换机
type Exchange struct {
	qname []string //绑定的队列
	ename string   //交换机名
	key   string   //routing-key
	form  string   //交换机类型
}

//NewConnect 连接
func NewConnect() *Connect {

	var c = new(Connect)
	var err error
	c.conn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Println("创建链接错误", err)
		return nil
	}
	return c
}

//NewExChange 创建实例
func NewExChange(ename, key, form string) Exchange {

	return Exchange{
		qname: make([]string, 10),
		ename: ename,
		key:   key,
		form:  form,
	}
}

//声明队列
func (c *Connect) createQueue(name string) amqp.Queue {

	queue, err := c.ch.QueueDeclare(
		name,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("创建队列失败：", err)
	}
	return queue
}

//声明交换机
func (c *Connect) createExcahnge(name, key string) {

	err := c.ch.ExchangeDeclare(
		name, //交换机名
		key,  //交换机类型
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("声明交换机失败：", err)
	}
}

//绑定
func (c *Connect) bind(qname, ename, key string) {

	err := c.ch.QueueBind(
		qname, //队列名，name
		key,   //routing key 交换类型，一个交换机可以绑定多个队列
		ename, //交换机名
		false,
		nil,
	)
	if err != nil {
		log.Fatal("声明交换机失败：", err)
	}
}

//SendMsg 发送消息
func (c *Connect) SendMsg(msg []byte, ename, key, form string) {

	//如果没有该交换机，就创建一个
	var k int
	var v Exchange
	for k, v = range c.exchange {
		if v.ename == ename {
			break
		}
	}
	if k == len(c.exchange) {
		c.createExcahnge(ename, key)
		c.exchange = append(c.exchange, NewExChange(ename, key, form))
	}

	err := c.ch.Publish(
		ename, //交换机名
		key,   //routing-key
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, //将消息持久化
			ContentType:  "text/plain",
			Body:         msg,
		},
	)
	if err != nil {
		log.Fatal("发送消息失败：", err)
	}
}

//Receive 接收消息
func (c *Connect) Receive(qname, ename, key string) []byte {

	//如果没有该队列，就创建一个
	for k := range c.queue {
		if k == qname {
			break
		}
	}
	c.createQueue(qname)

	//绑定
	c.bind(qname, ename, key)

	msg, err := c.ch.Consume(
		qname, //队列名
		"",    // consumer
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("发送消息失败：", err)
	}
	for b := range msg {
		fmt.Println(b.Body)
		b.Ack(false)
		return b.Body
	}
	return nil
}
