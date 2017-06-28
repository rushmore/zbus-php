<?php 
require_once '../zbus.php';

//start with a broker
$broker = new Broker("localhost:15555");


$p = new Producer($broker);

$msg = new Message();
$msg->topic = "MyTopic";
$msg->body = "from PHP";
 
$p->produce($msg); 


//close broker
$broker->close();

?> 