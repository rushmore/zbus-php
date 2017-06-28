<?php  

require_once'../zbus.php';

$broker = new Broker("localhost:15555");  

$producer = new Producer($broker);

?> 