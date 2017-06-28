<?php  
require_once '../zbus.php';
 

class MyService{  
	
	public function getString($msg){
		return $msg . ", From PHP";
	}
	
	public function testEncoding() {
		return "中文";
	}
	
	public function noReturn() {
		
	}
	
	public function plus($a, $b) {
		return $a + $b;
	}  
} 
 
$service = new MyService();

$processor = new RpcProcessor();
$processor->add_module($service);

 
$broker = new Broker("localhost:15555"); 
$c = new Consumer($broker, "MyRpc"); 
$c->message_handler = array($processor, 'message_handler');

$c->start();

$broker->close();

?> 