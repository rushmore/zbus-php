<?php 
require_once'../zbus.php';
$client = new MqClient("localhost:15555");

for($i=0;$i<20;$i++){
	
	$res = $client->query("MyTopic");
	  
	echo $res->topicName;
} 

?> 