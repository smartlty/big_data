#hello-world

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
  * Master为整个集群中的主节点
  * Master继承了Actor
  */
class Master extends Actor{
  println("constructor invoked")

  override def preStart(): Unit = {
    println("preStart invoked")
  }

  // 用于接收消息
  override def receive: Receive = {
    case "connect" => {
      println("a client connected")
      sender ! "reply"
    }

    case "hello" => {
      println("hello")
    }
  }
}

object Master {
  def main(args: Array[String]) {

    val host = args(0)
    val port = args(1).toInt
    // 准备配置
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    //ActorSystem老大，辅助创建和监控下面的Actor，他是单例的
    val actorSystem = ActorSystem("MasterSystem", config)
    //创建Actor, 起个名字
    val master = actorSystem.actorOf(Props[Master], "Master")//Master主构造器会执行
    master ! "hello"  //发送信息
    actorSystem.awaitTermination()  //让进程等待着, 先别结束
  }
}


----------------------------------------------------------------------------------
import akka.actor.{Props, ActorSystem, ActorSelection, Actor}
import com.typesafe.config.ConfigFactory

/**
  * Created by OracleOAEC on 2017/7/17.
  */
class Worker (val masterHost: String, val masterPort: Int) extends Actor{
  var master : ActorSelection = _
  //建立连接
  override def preStart(): Unit = {
    //在master启动时会打印下面的那个协议, 可以先用这个做一个标志, 连接哪个master
    //继承actor后会有一个context, 可以通过它来连接
    master = context.actorSelection(s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master") //需要有/user, Master要和master那边创建的名字保持一致
    master ! "connect"
  }

  override def receive: Receive = {
    case "reply" => {
      println("a reply form master")
    }
  }
}

object Worker {
  def main(args: Array[String]) {
    val host = args(0)
    val port = args(1).toInt
    val masterHost = args(2)
    val masterPort = args(3).toInt
    // 准备配置
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    //ActorSystem老大，辅助创建和监控下面的Actor，他是单例的
    val actorSystem = ActorSystem("WorkerSystem", config)
    actorSystem.actorOf(Props(new Worker(masterHost, masterPort)), "Worker")
    actorSystem.awaitTermination()
  }
}
