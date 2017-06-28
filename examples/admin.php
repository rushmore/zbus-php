<?php 
require_once'../zbus.php';
$broker = new Broker("localhost:15555");

$admin = new MqAdmin($broker);

$res = $admin->query("MyTopic");

foreach($res as $topic_info){
	echo json_encode($topic_info) . "\n";
}

$broker->close();

?> 