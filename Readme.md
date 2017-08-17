                /\\\                                                            /\\\                               
                \/\\\                                                           \/\\\                              
                 \/\\\                                               /\\\\\\\\\  \/\\\           /\\\\\\\\\        
     /\\\\\\\\\\\ \/\\\         /\\\    /\\\  /\\\\\\\\\\            /\\\/////\\\ \/\\\          /\\\/////\\\      
     \///////\\\/  \/\\\\\\\\\  \/\\\   \/\\\ \/\\\//////            \/\\\\\\\\\\  \/\\\\\\\\\\  \/\\\\\\\\\\      
           /\\\/    \/\\\////\\\ \/\\\   \/\\\ \/\\\\\\\\\\           \/\\\//////   \/\\\/////\\\ \/\\\//////      
          /\\\/      \/\\\  \/\\\ \/\\\   \/\\\ \////////\\\           \/\\\         \/\\\   \/\\\ \/\\\           
         /\\\\\\\\\\\ \/\\\\\\\\\  \//\\\\\\\\\   /\\\\\\\\\\           \/\\\         \/\\\   \/\\\ \/\\\          
         \///////////  \/////////    \/////////   \//////////            \///          \///    \///  \///          


zbus strives to make Message Queue and Remote Procedure Call fast, light-weighted and easy to build your own service-oriented architecture for many different platforms. Simply put, zbus = mq + rpc.

zbus carefully designed on its protocol and components to embrace KISS(Keep It Simple and Stupid) principle, but in all it delivers power and elasticity. 

- Working as MQ, compare it to RabbitMQ, ActiveMQ.
- Working as RPC, compare it to many more.

# zbus-php-client
NO threading in PHP client, HA not ready yet! Pull request if you are interested.

- Works for both PHP5 and PHP7
- zbus.php is the only source file required


## API Demo

Only demos the gist of API, more configurable usage calls for your further interest.

### Produce message

    $broker = new Broker("localhost:15555"); 
    $p = new Producer($broker);

    $msg = new Message();
    $msg->topic = "MyTopic";
    $msg->body = "from PHP";
    
    $p->produce($msg);  

    //close broker
    $broker->close();  

### Consume message

    $broker = new Broker("localhost:15555");
    $c = new Consumer($broker, "MyTopic");

    $c->message_handler = function($msg, $client){
        echo $msg . "\n";
    }; 

    $c->start();

    $broker->close();

### RPC client

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

### RPC service

    
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