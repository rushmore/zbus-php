<?php
require_once '../zbus.php';

$sock = socket_create(AF_INET, SOCK_STREAM, 0);

socket_connect($sock, 'localhost', 15555);
$msg = "GET /tracker HTTP/1.1 \r\n\r\n";

socket_send($sock, $msg, strlen($msg), 0);
socket_recv($sock, $buf, 10240, MSG_WAITALL);

echo $buf; 

 
$addr = new ServerAddress("localhost:15555", true);

$msg = new Message();
//$msg->status = 200;
$msg->url = "/tracker";
$msg->topic = "hong";
$msg->token = 'xxxxs';

$msg->set_json_body("jsonxxx");
$buf = (string)$msg;

echo $buf . PHP_EOL;

$msg2 = Message::decode($buf)[0];

echo $msg2;
 
?>