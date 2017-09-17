<?php  
require_once '../../zbus.php';
 
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
$processor->addModule($service);

 
$loop = new EventLoop();

$broker = new Broker($loop, "localhost:15555;localhost:15556"); 
$ctrl = new Message();
$ctrl->topic = "MyRpc";
$ctrl->topic_mask = Protocol::MASK_MEMORY | Protocol::MASK_RPC;
$c = new Consumer($broker, $ctrl); 
$c->connectionCount = 2;
$c->messageHandler = array($processor, 'messageHandler');

$c->start();
echo 'MyRpc service started' . PHP_EOL;
$loop->run();