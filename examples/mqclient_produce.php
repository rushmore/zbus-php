<?php 
require_once'../zbus.php';
$client = new MqClient("localhost:15555");

$msg = new Message();
$msg->topic = "MyTopic";
$msg->body = "Hi, from PHP, the Fxxking BEST language of the world, :)";

$res = $client->produce($msg) 

?> 