<?php   
require_once '../zbus.php';

$broker = new Broker("localhost:15555");
$c = new Consumer($broker, "MyTopic");

$c->message_handler = function($msg, $client){
	echo $msg . "\n";
};

$c->start();

$broker->close();
?> 