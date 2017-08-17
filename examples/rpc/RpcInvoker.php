<?php 

require_once '../../zbus.php';

function biz($broker) {
	$rpc = new RpcInvoker($broker, "MyRpc");
	
	//1) Raw invocation
	$req = new Request("plus", array(1, 2)); 
	$res = $rpc->invoke($req);
	echo $res->result . PHP_EOL; 
	
	//2) strong typed
	$res = $rpc->plus(1, 2);
	echo $res . PHP_EOL;
}


Logger::$Level = Logger::INFO;
$loop = new EventLoop(); 
$broker = new Broker($loop, "localhost:15555;localhost:15556", true); //sync mode 
$broker->on('ready', function() use($broker){ 
	try { 
		biz($broker); //run after ready
	} catch (Exception $e){
		echo $e->getMessage() . PHP_EOL;
	}
	$broker->close();  
});  
$loop->runOnce();