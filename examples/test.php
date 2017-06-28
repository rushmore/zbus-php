<?php  

require_once '../zbus.php';

class SocketEvent {
	public $sock;
	public $on_data;
	public $on_error; 
	
	public $recv_buf;
	public $send_buf; 
	
	public function __construct($sock){
		$this->sock = $sock;
	}
}


class IoLoop {
	public $socket_map;
	private $read_sockets = array();
	private $write_sockets = array();
	private $except_sockets = array();
	
	public function add_socket($socket_event){
		$this->socket_map[(string)$socket_event->sock] = $socket_event;
		socket_set_nonblock($socket_event->sock);
		array_push($this->read_sockets, $socket_event->sock);
	}
	
	
	public function start(){
		while(true){
			$reads = $this->read_sockets;
			$writes = $this->write_sockets;
			$excepts = $this->except_sockets;
			if(count($reads) < 1){
				sleep(1);
				continue;
			}
			
			$n = socket_select($reads, $writes, $excepts, 3); 
			if($n < 1) continue;  
			foreach($reads as $key=>$sock){
				while(true){
					$buf_len = 1024;
					$buf = socket_read($sock,$buf_len);  
					if($buf === false) break; 
				} 
			}
		}
	}
}

$client = new MqClient("localhost:15555");
$client->connect();

$client2 = new MqClient("localhost:15555");
$client2->connect();

$read = array($client->sock);
$msg = new Message();
$msg->cmd = "tracker";
$client->send($msg);  


$loop = new IoLoop();
$loop->add_socket(new SocketEvent($client->sock));
$loop->add_socket(new SocketEvent($client2->sock));

$loop->start();

?>