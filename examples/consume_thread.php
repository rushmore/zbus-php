<?php   
require_once'../zbus.php';
 
$timeout = 3;
$client = new MqClient("localhost:15555");
while(true){ 
	
	$res = $client->consume("MyTopic"); 
	
	echo $res->encode() . "\n";
} 

?> 