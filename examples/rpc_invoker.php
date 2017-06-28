<?php 

require_once '../zbus.php';
 
$broker = new Broker("localhost:15555"); 
$rpc = new RpcInvoker($broker, "MyRpc");

//1) Raw invocation
$req = new Request("plus", array(1, 2)); 
$res = $rpc->invoke($req); 
echo $res . "\n";


//2) Dynamic invocation
$res = $rpc->plus(1,2); 
echo $res . "\n";
 


$broker->close();

?> 