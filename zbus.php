<?php 

class Protocol {   
	//=============================[1] Command Values===============================================
	//MQ Produce/Consume
	const PRODUCE       = "produce";   
	const CONSUME       = "consume";  
	const ROUTE   	    = "route";     //route back message to sender, designed for RPC 
	const RPC   	    = "rpc";       //the same as produce command except rpc set ack false by default
	
	//Topic control
	const DECLARE_ = "declare";   //declare and empty keywords!!! PHP sucks
	const QUERY    = "query"; 
	const REMOVE   = "remove";
	const EMPTY_   = "empty";   
	
	//High Availability (HA) 
	const TRACK_PUB   = "track_pub"; 
	const TRACK_SUB   = "track_sub";  
	const TRACKER     = "tracker";    
	
	//=============================[2] Parameter Values================================================
	const COMMAND       	   = "cmd";     
	const TOPIC         	   = "topic";
	const TOPIC_MASK           = "topic_mask"; 
	const TAG   	     	   = "tag";  
	const OFFSET        	   = "offset";
	
	const CONSUME_GROUP        = "consume_group";  
	const GROUP_START_COPY     = "group_start_copy";  
	const GROUP_START_OFFSET   = "group_start_offset";
	const GROUP_START_MSGID    = "group_start_msgid";
	const GROUP_START_TIME     = "group_start_time";   
	const GROUP_FILTER         = "group_filter";  
	const GROUP_MASK           = "group_mask"; 
	const CONSUME_WINDOW       = "consume_window";  
	
	const SENDER   			= "sender"; 
	const RECVER   			= "recver";
	const ID      		    = "id";	   
	
	const HOST   		    = "host";  
	const ACK      			= "ack";	  
	const ENCODING 			= "encoding"; 
	
	const ORIGIN_ID         = "origin_id";
	const ORIGIN_URL   		= "origin_url";
	const ORIGIN_STATUS     = "origin_status";
	
	//Security 
	const TOKEN   		    = "token"; 
	
	const HEARTBEAT   		= "heartbeat"; 
	
	const MASK_MEMORY    	 = 1<<0;
    const MASK_RPC    	     = 1<<1;
    const MASK_PROXY    	 = 1<<2;
    const MASK_PAUSE    	 = 1<<3;
    const MASK_EXCLUSIVE 	 = 1<<4;
    const MASK_DELETE_ON_EXIT= 1<<5;
}

class ConsumeGroup {
	public $topic;        //topic of the group
	public $groupName;    
	public $filter; 
	public $startCopy;
	public $startOffset;  //create group start from offset, msgId to check valid
	public $startMsgId;
	public $startTime;    //unix time, create group start from time 
	
	function __construct($groupName = null, $filter = null) {  
		$this->groupName = $groupName;
		$this->filter = $filter;
	}
	
	public function fromMessage($msg){
		$this->topic = $msg->getHeader(Protocol::TOPIC);
		$this->groupName = $msg->getHeader(Protocol::CONSUME_GROUP);
		$this->filter = $msg->getHeader(Protocol::GROUP_FILTER);
		$this->startCopy = $msg->getHeader(Protocol::GROUP_START_COPY);
		$this->startOffset = $msg->getHeader(Protocol::GROUP_START_OFFSET);
		$this->startMsgId = $msg->getHeader(Protocol::GROUP_START_MSGID);
		$this->startTime = $msg->getHeader(Protocol::GROUP_START_TIME); 
	}
	
	public function toMessage($msg){
		$msg->setHeader(Protocol::TOPIC, $this->topic);
		$msg->setHeader(Protocol::CONSUME_GROUP, $this->groupName);
		$msg->setHeader(Protocol::GROUP_FILTER, $this->filter);
		$msg->setHeader(Protocol::GROUP_START_COPY, $this->startCopy);
		$msg->setHeader(Protocol::GROUP_START_OFFSET, $this->startOffset);
		$msg->setHeader(Protocol::GROUP_START_MSGID, $this->startMsgId);
		$msg->setHeader(Protocol::GROUP_START_TIME, $this->startTime);
	}
}

 
class Logger { 
	const DEBUG = 0;
	const INFO = 1;
	const WARN = 2;
	const ERROR = 3; 
	
	public static $Level = Logger::DEBUG; 
	
	public static function log($level, $message){
		if($level < Logger::$Level) return; 
		error_log($message);
	}  
	
	public static function debug($message){
		Logger::log(Logger::DEBUG, $message); 
	}
	public static function info($message){
		Logger::log(Logger::INFO, $message); 
	}
	public static function warn($message){
		Logger::log(Logger::WARN, $message); 
	}
	public static function error($message){
		Logger::log(Logger::ERROR, $message); 
	}
} 


//borrowed from: https://stackoverflow.com/questions/2040240/php-function-to-generate-v4-uuid
function uuid() {
	return sprintf( '%04x%04x-%04x-%04x-%04x-%04x%04x%04x', 
			mt_rand( 0, 0xffff ), mt_rand( 0, 0xffff ),  mt_rand( 0, 0xffff ), 
			mt_rand( 0, 0x0fff ) | 0x4000, mt_rand( 0, 0x3fff ) | 0x8000, 
			mt_rand( 0, 0xffff ), mt_rand( 0, 0xffff ), mt_rand( 0, 0xffff )
	);
} 


class ServerAddress {
	public $address;
	public $ssl_enabled;

	function __construct($address, $ssl_enabled= false) {  
		if(is_string($address)){
			$this->address = $address;
			$this->ssl_enabled= $ssl_enabled; 
			return;
		} else if (is_array($address)){
			$this->address = $address['address'];
			$this->ssl_enabled= $address['sslEnabled'];
		} else if (is_object($address) && get_class($address)== ServerAddress::class){
			$this->address = $address->address;
			$this->ssl_enabled= $address->ssl_enabled;
		} else {
			throw new Exception("address not support");
		}  
	}
	
	public function __toString(){
		if($this->ssl_enabled){
			return "[SSL]".$this->address;
		}
		return $this->address;
	}  
} 

const HTTP_STATUS_TABLE = array(
	200 => "OK",
	201 => "Created",
	202 => "Accepted",
	204 => "No Content",
	206 => "Partial Content",
	301 => "Moved Permanently",
	304 => "Not Modified",
	400 => "Bad Request",
	401 => "Unauthorized",
	403 => "Forbidden",
	404 => "Not Found",
	405 => "Method Not Allowed",
	416 => "Requested Range Not Satisfiable",
	500 => "Internal Server Error",
);

class Message {
	public $status;          //integer
	public $method = "GET";
	public $url = "/";
	public $headers = array();
	public $body;  
	
	
	public function removeHeader($name){
		if(!array_key_exists($name, $this->headers)) return;
		unset($this->headers[$name]);
	}
	
	public function getHeader($name, $value = null) {
		if(!array_key_exists($name, $this->headers)) return null;
		return $this->headers[$name];
	}
	
	public function setHeader($name, $value){
		if($value === null) return;
		$this->headers[$name] = $value;
	}
	
	public function setJsonBody($value){
		$this->headers['content-type'] = 'application/json';
		$this->body = $value; 
	}
	
	public function __set($name, $value){
		if($value === null) return;
		$this->headers[$name] = $value;
	}
	
	
	public function __get($name){
		if(!array_key_exists($name, $this->headers)) return null;
		return $this->headers[$name];
	}
	
	public function __toString(){
		return $this->encode();
	}
	
	
	public function encode(){
		$res = "";
		$desc = "unknown status";
		if($this->status){
			if(array_key_exists($this->status, HTTP_STATUS_TABLE)){
				$desc = HTTP_STATUS_TABLE[$this->status];
			}
			$res .= sprintf("HTTP/1.1 %s %s\r\n",$this->status, $desc);
		} else {
			$res .= sprintf("%s %s HTTP/1.1\r\n",$this->method?:"GET", $this->url?:"/");
		}
		foreach($this->headers as $key=>$value){
			if($key == 'content-length'){
				continue;
			}
			$res .= sprintf("%s: %s\r\n", $key, $value);
		}
		$body_len = 0;
		if($this->body){
			$body_len = strlen($this->body);
		}
		$res .= sprintf("content-length: %d\r\n", $body_len);
		$res .= sprintf("\r\n");
		if($this->body){
			$res .= $this->body;
		}
		
		return $res;
	}
	
	public static function decode($buf, $start=0){ 
		$p = strpos($buf, "\r\n\r\n", $start);
		if($p === false) return array(null, $start);
		$head_len = $p - $start;
		
		$head = substr($buf, $start, $head_len);
		$msg = Message::decodeHeaders($head);
		$body_len = 0;
		if(array_key_exists('content-length', $msg->headers)){
			$body_len = $msg->headers['content-length'];
			$body_len = intval($body_len);
		} 
		if( $body_len == 0) return array($msg, $p+4); 
		
		if(strlen($buf)-$p < $body_len){
			return array(null, $start);
		}
		$msg->body = substr($buf, $p+4, $body_len); 
		return array($msg, $p+4 + $body_len); 
	}
	 
	private static function decodeHeaders($buf){
		$msg = new Message();
		$lines = preg_split('/\r\n?/', $buf); 
		$meta = $lines[0];
		$blocks = explode(' ', $meta); 
		if(substr(strtoupper($meta), 0, 4 ) == "HTTP"){
			$msg->status = intval($blocks[1]);
		} else {
			$msg->method = strtoupper($blocks[0]);
			if(count($blocks) > 1){
				$msg->url = $blocks[1];
			}
		} 
		for($i=1; $i<count($lines); $i++){
			$line = $lines[$i]; 
			$kv = explode(':', $line); 
			if(count($kv) < 2) continue;
			$key = strtolower(trim($kv[0]));
			$val = trim($kv[1]); 
			$msg->headers[$key] = $val;
		} 
		return $msg;
	}   
}  
 

function buildMessage($topicCtrl, $cmd = null){
	if(is_string($topicCtrl)){
		$msg = new Message();
		$msg->topic = $topicCtrl;
	} else if(is_object($topicCtrl) && get_class($topicCtrl) == Message::class){
		$msg = $topicCtrl;
	} else if(is_object($topicCtrl) && get_class($topicCtrl) == ConsumeGroup::class){
		$msg = new Message();
		$topicCtrl->toMessage($msg);
	} else if(is_array($topicCtrl)){
		$msg = new Message();
		foreach($topicCtrl as $key => $val){
			$msg->setHeader($key, $val);
		}
	} else {
		throw new Exception("invalid: $topicCtrl");
	}  
	$msg->cmd = $cmd;
	return $msg;
}

class MqClient { 
	public $sock;
	public $token;
	
	private $serverAddress;
	private $sslCertFile;
	
	private $recvBuf;
	private $resultTable = array();
	
	function __construct($serverAddress, $sslCertFile=null){
		$this->serverAddress = new ServerAddress($serverAddress);
		$this->sslCertFile = $sslCertFile;
	}
	
	public function connect($timeout=3) {
		$address = $this->serverAddress->address;
		$bb = explode(':', $address);
		$host = $bb[0];
		$port = 80;
		if(count($bb) > 1){
			$port = intval($bb[1]);
		}
		
		Logger::debug("Trying connect to ($this->serverAddress)"); 
		$this->sock = socket_create(AF_INET, SOCK_STREAM, 0);
		if (!socket_connect($this->sock, $host, $port)){
			$this->throw_socket_exception("Connection to ($address) failed");
		}
		Logger::debug("Connected to ($this->serverAddress)");
	}
	
	public function close(){
		if($this->sock){
			socket_close($this->sock);
			$this->sock = null;
		}
	}
	
	private function throw_socket_exception($msgPrefix=null){
		$errorcode = socket_last_error($this->sock);
		$errormsg = socket_strerror($errorcode);
		$msg = "${msgPrefix}, $errorcode:$errormsg";
		Logger::error($msg);
		throw new Exception($msg);
	}

	
	public function invoke($msg, $timeout=3){
		$msgid = $this->send($msg, $timeout);
		return $this->recv($msgid, $timeout);
	}
	
	public function send($msg, $timeout=3){
		if($this->sock == null){
			$this->connect();
		}
		if($msg->id == null){
			$msg->id = uuid();
		}
		$buf = $msg->encode();
		Logger::debug($buf);
		$sendingBuf = $buf;
		$writeCount = 0;
		$totalCount = strlen($buf);
		while(true){
			$n = socket_write($this->sock, $sendingBuf, strlen($sendingBuf)); 
			if($n === false) {
				$this->throw_socket_exception("Socket write error");
			}
			$writeCount += $n;
			if($writeCount>=$totalCount) return;
			if($n > 0){
				$sendingBuf = substr($sendingBuf, $n);
			}
		}
		return $msg->id;
	}
	
	public function recv($msgid=null, $timeout=3){
		if($this->sock == null){
			$this->connect();
		}
		
		$allBuf = '';
		while(true) {
			if($msgid && array_key_exists($msgid, $this->resultTable)){
				return $this->resultTable[$msgid];
			}
			
			$bufLen = 4096;
			$buf = socket_read($this->sock, $bufLen);
			//$buf = fread($this->sock, $buf_len);
			if($buf === false || $buf == ''){
				$this->throw_socket_exception("Socket read error");
			}
			
			$allBuf .= $buf;
			
			$this->recvBuf .= $buf;
			$start = 0;
			while(true) {
				$res = Message::decode($this->recvBuf, $start);
				$msg = $res[0];
				$start = $res[1];
				if($msg == null) {
					if($start!= 0) {
						$this->recvBuf = substr($this->recvBuf, $start);
					}
					break;
				}
				$this->recvBuf = substr($this->recvBuf, $start);
				
				if($msgid != null){
					if($msgid != $msg->id){
						$this->resultTable[$msg->id] = $msg;
						continue;
					}
				}
				Logger::debug($allBuf);
				return $msg;
			}
		}
	}
	
	private function invokeCmd($cmd, $topicCtrl,$timeout=3){
		$msg = buildMessage($topicCtrl, $cmd);
		$msg->token = $this->token;
		return $this->invoke($msg, $timeout);
	}
	
	private function invokeObject($cmd, $topicCtrl, $timeout=3){
		$res = $this->invokeCmd($cmd, $topicCtrl, $timeout);
		if($res->status != 200){
			throw new Exception($res->body);
		}
		
		return json_decode($res->body);
	}
	
	
	public function produce($msg, $timeout=3) {
		return $this->invokeCmd(Protocol::PRODUCE, $msg, $timeout);
	}
	
	public function consume($topicCtrl, $timeout=3){
		$res = $this->invokeCmd(Protocol::CONSUME, $topicCtrl, $timeout);
		
		$res->id = $res->origin_id;
		$res->removeHeader(Protocol::ORIGIN_ID);
		if($res->status == 200){
			if($res->origin_url != null){
				$res->url = $res->origin_url;
				$res->status = null;
				$res->removeHeader(Protocol::ORIGIN_URL);
			}
		}
		return $res;
	}
	
	public function query($topicCtrl, $timeout=3){
		return $this->invokeObject(Protocol::QUERY, $topicCtrl, $timeout);
	}
	
	public function declare_($topicCtrl, $timeout=3){
		return $this->invokeObject(Protocol::DECLARE_, $topicCtrl, $timeout);
	}
	
	public function remove($topicCtrl, $timeout=3){
		return $this->invokeObject(Protocol::REMOVE, $topicCtrl, $timeout);
	}
	
	public function empty_($topicCtrl, $timeout=3){
		return $this->invokeObject(Protocol::EMPTY_, $topicCtrl, $timeout);
	}
	
	public function route($msg, $timeout=3){
		$msg->cmd = Protocol::ROUTE;
		if($msg->status != null){
			$msg->set_header(Protocol::ORIGIN_STATUS, $msg->status);
			$msg->status = null;
		}
		return $this->send($msg, $timeout);
	}  
	
}


class MqClientAsync { 
	use EventEmitter;
	
	public $token;
	public $serverAddress;
	
	private $stream;
	private $loop; 
	private $sslCertFile;
	
	private $recvBuffer;
	private $callbackTable = array();
	
	private $heartbeator;
	private $heartbeatInterval = 60; //60seconds
	private $connectTimeout = 3;
	private $autoReconnect = true;
	private $connectTimer;
	
	function __construct($address, $loop, $sslCertFile=null, $heartbeatInterval=60){
		$this->serverAddress= new ServerAddress($address);
		$this->loop = $loop;
		$this->sslCertFile= $sslCertFile;
		
		$this->heartbeatInterval = $heartbeatInterval;
		$that = $this;
		
		$this->heartbeator = $loop->addTimer($this->heartbeatInterval, function() use($that){
			$that->heartbeat();
		}, true);
	}
	
	public function fork(){
		return new MqclientAsync($this->serverAddress, $this->loop, $this->sslCertFile, $this->heartbeatInterval);
	}
	
	public function connect(callable $connected = null) {
		$address = $this->serverAddress->address;
		Logger::debug('Trying connect to ' . $address);
		$context = array();
		$errno = null;
		$errstr = null;
		$socket = @stream_socket_client(
				'tcp://'.$address,
				$errno,
				$errstr,
				0,
				STREAM_CLIENT_ASYNC_CONNECT | STREAM_CLIENT_CONNECT,
				stream_context_create($context)
				);
		
		if($socket === false) {
			$this->emit('error', array(new Exception("Connection to ($address) failed, $errstr")));
			return;
		} 
		
		$client = $this; 
		$this->stream = null;
		
		$this->connectTimer = $this->loop->addTimer($this->connectTimeout, function() 
				use($client, $socket, $connected, $address){  
			
			if(is_resource($socket) && stream_socket_get_name($socket, true) === false){
				$client->loop->removeWriteStream($socket);
				fclose($socket);
			}   
			
			if ($client->stream == null) {
				Logger::warn('Connection (' . $address . ') timeout');
				if($client->autoReconnect){
					$client->connect($connected);
				}
			}  
		});
		
		$this->loop->addWriteStream($socket, function($socket) use($client, $connected, $address){ 
			$client->loop->removeWriteStream($socket);
			
			if (stream_socket_get_name($socket, true) === false ) { 
				fclose($socket); 
				return;
			}  
			Logger::debug('Connected to (' . $address . ')');
			$client->createStream($socket, $connected);
		});
	}
	
	private function createStream($socket, $connected) {	 
		$client = $this;
		$client->stream = new Stream($socket, $client->loop); 
		if($connected){
			$connected();
		}
		$client->emit('connected'); 
		
		$client->stream->on('data', function($data) use($client) {
			$client->recvBuffer .= $data;
			$start = 0;
			while(true) {
				$res = Message::decode($client->recvBuffer, $start);
				$msg = $res[0];
				$start = $res[1];
				if($msg === null) {
					if($start != 0) {
						$client->recvBuffer = substr($client->recvBuffer, $start);
					}
					break;
				}
				
				$callback = @$client->callbackTable[$msg->id];
				if($callback !== null){
					try{
						unset($client->callbackTable[$msg->id]);
						$callback($msg);
					} catch (Exception $e){
						Logger::error($e->getMessage());
					}
				} else {
					$client->emit('message', array($msg));
				}
			}
			
		});
			
		$client->stream->on('error', function($data) use($client){
			$client->emit('error', array($data));
		});
			
		$client->stream->on('close', function($data) use($client){
			$client->emit('close', array($data));
		});
			
		$client->stream->on('drain', function() use($client){
			$client->emit('drain', array());
		});  
	} 
	
	public function close(){
		if($this->stream !== null){
			$this->stream->close();
		} 
		$this->loop->cancelTimer($this->connectTimer);
		$this->loop->cancelTimer($this->heartbeator);
	}
	
	protected function heartbeat(){
		if($this->stream == null || !$this->stream->isActive()) return;
		$msg = new Message();
		$msg->cmd = Protocol::HEARTBEAT;
		$this->invoke($msg);
	}
	
	public function invoke($msg, callable $callback = null){
		if($msg->id == null){
			$msg->id = uuid();
		}
		if($callback){
			$this->callbackTable[$msg->id] = $callback;
		}
		
		$buf = $msg->encode(); 
		$this->stream->write($buf);
	}
	
	
	private function invokeCmd($cmd, $topicCtrl, callable $callback = null){
		$msg = buildMessage($topicCtrl, $cmd);
		$msg->token = $this->token;
		return $this->invoke($msg, $callback);
	}
	
	private function invokeObject($cmd, $topicCtrl, callable $callback = null){
		$this->invokeCmd($cmd, $topicCtrl, function($res) use($callback){ 
			if($res->status != 200){
				$error = new Exception($res->body); 
				$callback(array('error' => $error)); 
				return;
			} 
			try{
				$obj = json_decode($res->body);
				$callback($obj); 
			} catch (Exception $e){
				$callback(array('error' => $e)); 
			}
		});
	}
	
	public function query($topicCtrl, callable $callback = null){
		return $this->invokeObject(Protocol::QUERY, $topicCtrl, $callback);
	}
	
	public function declare_($topicCtrl, callable $callback = null){
		return $this->invokeObject(Protocol::DECLARE_, $topicCtrl, $callback);
	}
	
	public function remove($topicCtrl, $callback = null){
		return $this->invokeCmd(Protocol::REMOVE, $topicCtrl, $callback);
	}
	
	public function empty_($topicCtrl, $callback= null){
		return $this->invokeCmd(Protocol::EMPTY_, $topicCtrl, $callback); 
	}
	
	public function produce($msg, callable $callback=null) {
		if($callback == null){
			$msg->ack = false;
		}
		return $this->invokeCmd(Protocol::PRODUCE, $msg, $callback);
	}
	
	public function consume($topicCtrl, callable $callback){
		$this->invokeCmd(Protocol::CONSUME, $topicCtrl, function($res) use($callback){
			$res->id = $res->origin_id;
			$res->removeHeader(Protocol::ORIGIN_ID);
			if($res->status == 200){
				if($res->origin_url != null){
					$res->url = $res->origin_url;
					$res->removeHeader(Protocol::ORIGIN_URL);
				}
			}
			if($callback) $callback($res);
		});
	}
	
	public function route($msg){
		$msg->cmd = Protocol::ROUTE;
		if($msg->status != null){
			$msg->setHeader(Protocol::ORIGIN_STATUS, $msg->status);
			$msg->status = null;
		}
		return $this->invoke($msg);
	}
} 
 
class BrokerRouteTable {
	public $topicTable = array();   //{ TopicName => [TopicInfo] }
	public $serverTable = array();  //{ ServerAddress => ServerInfo }
	public $votesTable = array();   //{ TrackerAddress => Vote } , Vote=(version, servers) 
	public $voteFactor = 0.5;
	
	
	private $votedTrackers = array(); // { TrackerAddress => true }
	
	public function updateTracker($trackerInfo){
		//1) Update votes
		$trackerAddress = new ServerAddress($trackerInfo['serverAddress']);
		$vote = @$this->votesTable[$trackerAddress];
		$this->votedTrackers[(string)$trackerAddress] = true;
		
		if($vote && $vote['version'] >=  $trackerInfo['infoVersion']){
			return;
		}
		$servers = array();
		$serverTable = $trackerInfo['serverTable'];
		foreach($serverTable as $key => $serverInfo){ 
			array_push($servers, new ServerAddress($serverInfo['serverAddress']));
		}
		 
		$this->votesTable[(string)$trackerAddress] = array('version'=>$trackerInfo['infoVersion'], 'servers'=>$servers);
		
		//2) Merge ServerTable
		foreach($serverTable as $key => $serverInfo){
			$serverAddress = new ServerAddress($serverInfo['serverAddress']);
			$this->serverTable[(string)$serverAddress] = $serverInfo; 
		} 
		
		//3) Purge
		return $this->purge();
	}
	
	public function removeTracker($trackerAddress){
		$trackerAddress = new ServerAddress($trackerAddress);
		unset($this->votesTable[(string)$trackerAddress]);
		return $this->purge();
	}
	
	private function purge(){
		$toRemove = array(); 
		$serverTableLocal = $this->serverTable;
		foreach($serverTableLocal as $key => $server_info){
			$serverAddress = new ServerAddress($server_info['serverAddress']); 
			$count = 0;
			foreach($this->votesTable as $key => $vote){
				$servers = $vote['servers'];
				if(in_array((string)$serverAddress, $servers)) $count++;
			}
			if($count < count($this->votedTrackers)*$this->voteFactor){
				array_push($toRemove, $serverAddress);
				unset($serverTableLocal[(string)$serverAddress]);
			} 
		} 
		$this->serverTable = $serverTableLocal;
		
		$this->rebuildTopicTable();  
		return $toRemove;
	}
	
	private function rebuildTopicTable(){
		$topicTable = array();
		foreach($this->serverTable as $server_key => $serverInfo){
			foreach($serverInfo['topicTable'] as $topicKey => $topicInfo){
				$topicList = @$topicTable[$topicKey];
				if($topicList == null){
					$topicList = array();
				}
				array_push($topicList, $topicInfo);
				$topicTable[$topicKey] = $topicList;
			}
		}
		$this->topicTable = $topicTable;
	}
}

class TrackerSubscriber {
	public $client;
	public $readyCount = 0;
	public $readyTriggered = false;

	public function __construct($client){
		$this->client = $client;
	}
}

class Broker {  
	use EventEmitter;
	
	public $routeTable;
	 
	private $syncEnabled = false;  
	private $clientTable = array(); //store MqClientAsync or MqClient
	
	private $sslCertFileTable = array();
	private $loop;
	private $autoReconnectTimeout = 3; 

	private $trackerSubscribers = array();  
	private $readyTriggered = false; //any tracker tiggered is considered broker ready triggered.
	
	public function __construct(EventLoop $loop, $trackerAddressList = null, $syncEnabled = false){ 
		$this->loop = $loop;
		$this->routeTable = new BrokerRouteTable();
		$this->syncEnabled = $syncEnabled;
		
		if($trackerAddressList){
			$bb = explode(';', $trackerAddressList);
			foreach($bb as $trackerAddress){
				$this->addTracker($trackerAddress);
			} 
		}
	}
	
	public function addTracker($trackerAddress, $sslCertFile=null){
		$client = new MqClientAsync($trackerAddress, $this->loop, $sslCertFile);
		$trackerSubscriber = new TrackerSubscriber($client);
		$this->trackerSubscribers[$trackerAddress] = $trackerSubscriber;
		$broker = $this;
		$remoteTrackerAddress = $trackerAddress;
		$client->on('message', function($msg) use($broker, $trackerSubscriber, &$remoteTrackerAddress, $sslCertFile){ 
			if($msg->status != 200){
				Logger::error('track_sub status warning');
				return;
			}
			
			$trackerInfo = json_decode($msg->body, true); 
			
			$remoteTrackerAddress = new ServerAddress($trackerInfo['serverAddress']); 
			if($sslCertFile) {
				$broker->sslCertFileTable[(string)$remoteTrackerAddress] = $sslCertFile;
			}
			if(@$this->trackerSubscribers[(string)$remoteTrackerAddress] === null){
				$this->trackerSubscribers[(string)$remoteTrackerAddress] = $trackerSubscriber;
			}
			if (!$trackerSubscriber->readyTriggered){
				$trackerSubscriber->readyCount = count($trackerInfo['serverTable']);
			}

			$toRemove = $this->routeTable->updateTracker($trackerInfo);
			$serverTable = $broker->routeTable->serverTable;
			foreach ($serverTable as $key=>$serverInfo){
				$broker->addServer($serverInfo, $trackerSubscriber);
			}
			foreach ($toRemove as $key=>$serverAddress){
				$broker->removeServer($serverAddress);
			} 
			
			$broker->emit('trackerUpdate', array($broker)); 
		});
		
		$client->on('close', function() use($client, $broker, &$remoteTrackerAddress) { 
			$toRemove = $broker->routeTable->removeTracker($remoteTrackerAddress);
			foreach ($toRemove as $key => $serverAddress){
				$broker->removeServer($serverAddress);
			} 
		});
		
		$client->on('connected', function() use($client){
			$msg = new Message();
			$msg->cmd = Protocol::TRACK_SUB;
			$client->invoke($msg);
		});  
		
		$client->on('error', function($error) use($client, $broker){
			Logger::error($error->getMessage());
			$broker->loop->addTimer($broker->autoReconnectTimeout, function() use($client) { 
				$client->connect();
			});
		});
		
		$client->connect();  
	} 
	 
	private function addServer($serverInfo,  $trackerSubscriber) { 
		$serverAddress = new ServerAddress($serverInfo['serverAddress']);
		$client = @$this->clientTable[(string)$serverAddress];
		if($client !== null){
			return; //client already exists
		} 
		$sslCertFile = @$this->sslCertFileTable[(string)$serverAddress];
		$client = $this->createClient($serverAddress, $this->loop, $sslCertFile); 
		$this->clientTable[(string)$serverAddress] = $client;
		$broker = $this;
		
		if($broker->syncEnabled){ //for sync mode
			$broker->emit('serverJoin', array($client));
			if(!$trackerSubscriber->readyTriggered) {
				$trackerSubscriber->readyCount--;
				if($trackerSubscriber->readyCount <= 0) {
					if(!$broker->readyTriggered){
						$broker->emit('ready');
						$broker->readyTriggered = true;
					}
					$trackerSubscriber->readyTriggered = true;
				}
			}  
			return;
		}
		
		//async MqClient 
		$client->on('connected', function() use($broker, $client, $serverAddress,  $trackerSubscriber) {  
			$broker->emit('serverJoin', array($client));
			if(!$trackerSubscriber->readyTriggered) {
				$trackerSubscriber->readyCount--;
				if($trackerSubscriber->readyCount <= 0) {
					if(!$broker->readyTriggered){
						$broker->emit('ready'); 
						$broker->readyTriggered = true;
					} 
					$trackerSubscriber->readyTriggered = true; 
				}
			} 
		});
		$client->connect();
	}
	 
	
	private function removeServer($serverAddress) {  
		$client = @$this->clientTable[(string)$serverAddress];
		if($client === null){
			return; 
		}
		$this->emit('serverLeave', array($serverAddress));
		unset($this->clientTable[(string)$serverAddress]);
		$client->close();
	}
	
	private function createClient($serverAddress, $sslCertFile=null){
		if($this->syncEnabled){
			return new MqClient($serverAddress, $sslCertFile);
		}
		return new MqClientAsync($serverAddress, $this->loop, $sslCertFile);
	}
	 

	public function select($selector, $msg){
		$addressList = $selector($this->routeTable, $msg);
		if(!is_array($addressList)){
			$addressList = array($addressList);
		}
		
		$clientSelected = array();
		foreach($addressList as $address){
			$client = @$this->clientTable[(string)$address];
			if($client == null){ 
				Logger::warn("Missing client for " . $address);
				continue;
			}
			array_push($clientSelected, $client);
		} 
		return $clientSelected;
	} 
	
	public function close(){
		foreach($this->trackerSubscribers as $key=>$sub){
			$sub->client->close();
		}
		$this->trackerSubscribers = array();
		
		foreach($this->clientTable as $key=>$client){
			$client->close();
		}
		$this->clientTable = array();
	} 
	
	public function isSync(){
		return $this->syncEnabled;
	}
}

class MqAdmin { 
	protected $broker; 
	protected $adminSelector;
	protected $token;
	public function __construct($broker){
		$this->broker = $broker;
		$this->adminSelector = function($routeTable, $msg){
			$serverTable = $routeTable->serverTable;
			$addressArray = array();
			foreach($serverTable as $key => $serverInfo){
				$serverAddress = new ServerAddress($serverInfo['serverAddress']);
				array_push($addressArray, $serverAddress);
			}
			return $addressArray;
		};
	} 
	
	private function invokeCmdAsync($cmd, $topicCtrl, callable $callback, $selector=null){ 
		if($this->broker->isSync()){
			throw new Exception("async should be enabled in broker");
		}
		$msg = buildMessage($topicCtrl, $cmd);
		if($msg->token == null) $msg->token = $this->token;
		
		if($selector == null) $selector = $this->adminSelector;
		$clientArray = $this->broker->select($selector, $msg); 
		foreach ($clientArray as $client){ 
			$client->invoke($msg, $callback);  
		} 
	} 
	
	private function invokeObjectAsync($cmd, $topicCtrl, callable $callback, $selector=null){ 
		if($this->broker->isSync()){
			throw new Exception("async should be enabled in broker");
		}
		
		$this->invokeCmdAsync($cmd, $topicCtrl, function($msg) use($callback){
			$data = null;
			if($msg->status != 200){
				$data= new Exception($msg->body);
			} else {
				$data = json_decode($msg->body);
			}
			call_user_func($callback, $data);
		}, $selector);  
	}
	
	private function invokeCmd($cmd, $topicCtrl, $timeout = 3, $selector=null){
		if(!$this->broker->isSync()){
			throw new Exception("sync should be enabled in broker");
		}
		$msg = buildMessage($topicCtrl, $cmd);
		if($msg->token == null) $msg->token = $this->token;
		
		if($selector == null) $selector = $this->adminSelector;
		$clientArray = $this->broker->select($selector, $msg);
		$resArray = array();
		foreach ($clientArray as $client){
			$res = $client->invoke($msg, $timeout);
			array_push($resArray, $res);
		}
		return $resArray;
	}
	
	private function invokeObject($cmd, $topicCtrl, $timeout = 3, $selector=null){
		if(!$this->broker->isSync()){
			throw new Exception("sync should be enabled in broker");
		}
		
		$msgArray = $this->invokeCmd($cmd, $topicCtrl, $timeout, $selector);
		$resArray = array();
		foreach($msgArray as $key=>$msg){
			$res = null;
			if($msg->status != 200){
				$res = new Exception($msg->body);
			} else {
				$res = json_decode($msg->body);
			}
			array_push($resArray, $res);
		}
		return $resArray;
	}
	
	public function query($topicCtrl, $timeout = 3, $selector=null){
		return $this->invokeObject(Protocol::QUERY, $topicCtrl, $timeout, $selector);
	}
	
	public function queryAsync($topicCtrl, callable $callback, $selector=null){  
		$this->invokeObjectAsync(Protocol::QUERY, $topicCtrl, $callback, $selector);
	}
	
	public function declare_($topicCtrl, $timeout = 3, $selector=null){
		return $this->invokeObject(Protocol::DECLARE_, $topicCtrl, $timeout, $selector);
	}
	
	public function declareAsync($topicCtrl, callable $callback, $selector=null){ 
		$this->invokeObjectAsync(Protocol::DECLARE_, $topicCtrl, $callback, $selector);
	}
	
	public function remove($topicCtrl, $timeout = 3, $selector=null){
		return $this->invokeCmd(Protocol::REMOVE, $topicCtrl, $timeout, $selector);
	}
	
	public function removeAsync($topicCtrl, callable $callback, $selector=null){ 
		$this->invokeCmdAsync(Protocol::REMOVE, $topicCtrl, $callback, $selector);
	}
	public function empty_($topicCtrl, $timeout = 3, $selector=null){
		return $this->invokeCmd(Protocol::EMPTY_, $topicCtrl, $timeout, $selector);
	}  
	
	public function emptyAsync($topicCtrl, callable $callback, $selector=null){ 
		$this->invokeCmdAsync(Protocol::EMPTY_, $topicCtrl, $callback, $selector);
	}  
}

class Producer extends  MqAdmin{
	protected $produceSelector;
	
	public function __construct($broker){
		parent::__construct($broker);
		
		$this->produceSelector= function($routeTable, $msg){
			if($msg->topic == null){
				throw new Exception("Missing topic");
			}
			if(count($routeTable->serverTable) < 1) {
				return array();
			}
			$topicTable = $routeTable->topicTable;
			$serverList = @$topicTable[$msg->topic];
			if($serverList == null || count($serverList) < 1){
				return array();
			}
			$target = null;
			foreach($serverList as $topicInfo){
				if($target == null){
					$target = $topicInfo;
					continue;
				}
				if($target['consumerCount']<$topicInfo['consumerCount']){
					$target = $topicInfo;
				}
			}
			$res = array();
			array_push($res, new ServerAddress($target['serverAddress']));
			return $res;
		};
	}
	
	public function publishAsync($msg, callable $callback, $selector=null){
		if($selector == null) $selector = $this->produceSelector;
		
		$msg->cmd = Protocol::PRODUCE;
		if($msg->token == null) $msg->token = $this->token;
		
		$clientArray = $this->broker->select($selector, $msg);
		if(count($clientArray) < 1){
			throw new Exception("Missing MqServer for $msg");
		}
		
		foreach ($clientArray as $key => $client){
			$client->invoke($msg, $callback); 
		} 
	}
	
	public function publish($msg, $timeout = 3, $selector=null){
		if($selector == null) $selector = $this->produceSelector;
		
		$msg->cmd = Protocol::PRODUCE;
		if($msg->token == null) $msg->token = $this->token;
		
		$clientArray = $this->broker->select($selector, $msg);
		if(count($clientArray) < 1){
			throw new Exception("Missing MqServer for $msg");
		}
		
		$resArray = array();
		foreach ($clientArray as $key => $client){
			$res = $client->invoke($msg, $timeout );
			array_push($resArray, $res);
		}
		if(count($resArray) == 1){
			return $resArray[0];
		}
		return $resArray;
	}
	
}

class Consumer extends  MqAdmin{
	public $messageHandler;
	public $topic;
	public $consumeGroup;
	
	public $consumeSelector;
	public $connectionCount = 1;
	public $consumeClientTable = array();
	
	
	public function __construct($broker, $topic, $consumeGroup = null){
		parent::__construct($broker);
		$this->topic = $topic;
		$this->consumeGroup = $consumeGroup;
		if($this->consumeGroup == null){
			$this->consumeGroup = new ConsumeGroup();
		}
		
		$this->consumeSelector = function($routeTable, $msg){
			$serverTable = $routeTable->serverTable;
			$addressArray = array();
			foreach($serverTable as $key => $serverInfo){
				$serverAddress = new ServerAddress($serverInfo['serverAddress']);
				array_push($addressArray, $serverAddress);
			}
			return $addressArray;
		};
	} 
	
	public function start(){   
		$c = $this;
		$this->broker->on('serverJoin', function($client) use($c){ 
			$c->consumeToServer($client);
		});
		
		$this->broker->on('serverLeave', function($serverAddress) use($c){
			$c->leaveServer($serverAddress);
		}); 
	}
	
	private function consumeToServer($client){ 
		$serverAddress = $client->serverAddress;
		$clientList= @$this->consumeClientTable[(string)$serverAddress];
		if($clientList !== null) {
			return;
		} 
		
		$msg = buildMessage($this->topic);
		$this->consumeGroup->topic = $msg->topic;
		$this->consumeGroup->toMessage($msg); 
		$msg->token = $this->token; 
		
		$clientList = array(); 
		for($i=0; $i<$this->connectionCount;$i++){
			$forkedClient = $client->fork();
			array_push($clientList, $forkedClient);
			$this->consume($forkedClient, $msg); 
		}  
		$this->consumeClientTable[(string)$serverAddress] = $clientList; 
	}
	
	private function leaveServer($serverAddress){
		$clientList = @$this->consumeClientTable[(string)$serverAddress];
		if($clientList === null) return;
		
		foreach ($clientList as $key => $client){
			$client->close();
		} 
		unset($this->consumeClientTable[(string)$serverAddress]);
	}
	
	private function consume($client, $consumeCtrl){
		$consumer = $this;
		$client->on('connected', function() use($consumer, $client, $consumeCtrl){
			$client->declare_($consumeCtrl, function($res) use($consumer, $client, $consumeCtrl){
				if(is_a($res, 'Exception')){
					Logger::error('Declare error: ' . $res->getMessage());
					return;
				}
				$client->consume($consumeCtrl, function($res) use($consumer, $client, $consumeCtrl){
					$consumer->consumeCallback($client, $consumeCtrl, $res);
				});
			});
		});
			
		$client->connect();
	}
	
	private function consumeCallback($client, $consumeCtrl, $res){
		$consumer = $this;
		if($res->status == 404){
			$client->declare_($consumeCtrl, function($res) use($consumer, $client, $consumeCtrl){
				if(is_a($res, 'Exception')){
					Logger::error('Declare error: ' . $res->getMessage());
					return;
				}
				$client->consume($consumeCtrl, function($res) use($consumer, $client,$consumeCtrl){
					$consumer->consumeCallback($client, $consumeCtrl, $res);
				});
			});
			return;
		}
		
		$originUrl = $res->origin_url;
		$id = $res->origin_id;
		$res->removeHeader('origin_url');
		$res->removeHeader('origin_id');
		if($originUrl !== null){
			$res->url = $originUrl;
		}
		$res->id = $id;
		
		if($this->messageHandler !== null){
			try{
				call_user_func($this->messageHandler, $res, $client); 
			} catch (Exception $e){
				Logger::warn($e->getMessage());
			} finally {
				$client->consume($consumeCtrl, function($res) use($consumer,$client, $consumeCtrl){
					$consumer->consumeCallback($client, $consumeCtrl, $res);
				});
			}
		}
	} 
} 



class Request{
	public $method;
	public $params;
	public $module;
	
	public function __construct($method=null, $params=null, $module=null){
		$this->method = $method;
		$this->params = $params;
		$this->module = $module;
	}
}
class Response{
	public $result;
	public $error;
	
	public function __construct($result=null, $error=null){
		$this->result = $result;
		$this->error = $error;
	}
	
	public function __toString(){
		return json_encode($this);
	}
}

class RpcException extends Exception { 
	public function __construct($message, $code = 0, Exception $previous = null) { 
		parent::__construct($message, $code, $previous);
	}
	 
	public function __toString() {
		return __CLASS__ . ": [{$this->code}]: {$this->message}\n";
	} 
}

class RpcInvoker {
	public $producer;
	public $token;
	
	public $topic;
	public $module;
	public $rpcSelector;
	public $rpcTimeout = 3;
	
	private $broker;
	
	public function __construct($broker, $topic){ 
		$this->broker = $broker;
		$this->producer = new Producer($broker);
		$this->topic = $topic;
	} 
	
	public function __call($method, $args){
		if($this->broker->isSync()){
			return $this->callSync($method, $args);
		}
		
		$this->callAsync($method, $args);
	}
	
	private function callSync($method, $args){ 
		$request = new Request($method, $args);
		$request->module = $this->module;
		
		$response = $this->invoke($request, $this->rpcTimeout, $this->rpcSelector);
		if($response->error != null){
			throw new RpcException((string)$response->error);
		}
		return $response->result; 
	}
	
	
	private function callAsync($method, $args){
		$params = array_slice($args, 0, count($args)-1);
		$callback = $args[count($args)-1];
		
		$request = new Request($method, $params);
		$request->module = $this->module;
		
		$this->invokeAsync($request, function($response) use($callback){
			if($response->error != null){
				if(is_object($response) && is_a($response, Exception::class)){
					$error = $response->error;
				} else {
					$error = new RpcException((string)$response->error);
				}
				
				call_user_func($callback, $error);
				return;
			}
			call_user_func($callback, $response->result);
		}, $this->rpcSelector);
	}
	
	public function invokeAsync($request, callable $callback, $selector=null){
		if($selector == null) $selector = $this->rpcSelector; 
		
		$msg = new Message();
		$msg->topic = $this->topic;
		$msg->token = $this->token; 
		$msg->ack = 0;
		
		$rpcBody = json_encode($request);
		$msg->setJsonBody($rpcBody);
		
		$this->producer->publishAsync($msg, function($msgRes) use($callback){
			if($msgRes->status != 200){
				$res = new RpcException($msgRes->body);
				call_user_func($callback, $res);
				return;
			}
			
			$arr = json_decode($msgRes->body, true);
			$res = new Response();
			$res->error = @$arr['error'];
			$res->result = @$arr['result'];
			
			call_user_func($callback, $res);
			
		}, $selector);  
	} 
	
	public function invoke($request, $timeout=3, $selector=null){
		if($selector == null) $selector = $this->rpcSelector;
		
		$msg = new Message();
		$msg->topic = $this->topic;
		$msg->token = $this->token;
		$msg->ack = 0;  //RPC no need ack
		
		$rpcBody = json_encode($request);
		$msg->setJsonBody($rpcBody);
		
		$msgRes = $this->producer->publish($msg, $timeout, $selector); 
		if($msgRes->status != 200){
			throw new RpcException($msgRes->body); 
		}
		
		$arr = json_decode($msgRes->body, true);
		$res = new Response();
		$res->error = @$arr['error'];
		$res->result = @$arr['result'];
		return $res;
	} 
}

class RpcProcessor {
	private $methods = array(); 
	
	public function addModule($service, $module=null){ 
		if(is_string($service)){
			$service = new $service();
		}
		$serviceClass = get_class($service);
		$class = new ReflectionClass($serviceClass);
		$methods = $class->getMethods(ReflectionMethod::IS_PUBLIC & ~ReflectionMethod::IS_STATIC);
		foreach($methods as $method){ 
			$key = $this->genKey($module, $method->name);
			$this->methods[$key] = array($method, $service); 
		} 
	}
	
	private function genKey($module, $method_name){
		return "$module:$method_name";
	}
	
	private function process($request){
		$key = $this->genKey($request->module, $request->method);
		$m = @$this->methods[$key];
		if($m == null){
			throw new ErrorException("Missing method $key");
		}
		$args = $request->params;
		if($args === null){
			$args = array();
		}
		$response = new Response();
		try{
			$response->result = $m[0]->invokeArgs($m[1], $args); 
		} catch (Exception $e){
			$response->error = $e;
		}
		return $response; 
	}
	
	
	public function messageHandler($msg, $client){
		$msgRes = new Message();
		$msgRes->recver = $msg->sender;
		$msgRes->id = $msg->id;
		$msgRes->status = 200;
		
		$response = new Response();
		try{
			$json = json_decode($msg->body, true);
			$request = new Request();
			$request->method = @$json['method'];
			$request->params = @$json['params'];
			$request->module = @$json['module'];
			
			$response = $this->process($request); 
			
		} catch (Exception $e){
			$response->error = $e->getMessage();
		}
		$jsonRes = json_encode($response);
		$msgRes->setJsonBody($jsonRes);
		
		$client->route($msgRes);
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
//EventLoop support in the following
////////////////////////////////////////////////////////////////////////////////////////////////////////
trait EventEmitter {
	protected $listeners = [];
	protected $onceListeners = [];
	
	public function on($event, callable $listener) {
		if (!isset($this->listeners[$event])) {
			$this->listeners[$event] = [];
		}
		$this->listeners[$event][] = $listener;
		return $this;
	}
	
	public function once($event, callable $listener) {
		if (!isset($this->onceListeners[$event])) {
			$this->onceListeners[$event] = [];
		}
		$this->onceListeners[$event][] = $listener;
		return $this;
	}
	
	public function removeListener($event, callable $listener){
		if (isset($this->listeners[$event])) {
			$index = \array_search($listener, $this->listeners[$event], true);
			if (false !== $index) {
				unset($this->listeners[$event][$index]);
				if (\count($this->listeners[$event]) === 0) {
					unset($this->listeners[$event]);
				}
			}
		}
		if (isset($this->onceListeners[$event])) {
			$index = \array_search($listener, $this->onceListeners[$event], true);
			if (false !== $index) {
				unset($this->onceListeners[$event][$index]);
				if (\count($this->onceListeners[$event]) === 0) {
					unset($this->onceListeners[$event]);
				}
			}
		}
	}
	
	public function removeAllListeners($event = null) {
		if ($event !== null) {
			unset($this->listeners[$event]);
		} else {
			$this->listeners = [];
		}
		if ($event !== null) {
			unset($this->onceListeners[$event]);
		} else {
			$this->onceListeners = [];
		}
	}
	
	public function listeners($event) {
		return array_merge(
				isset($this->listeners[$event]) ? $this->listeners[$event] : [],
				isset($this->onceListeners[$event]) ? $this->onceListeners[$event] : []
				);
	}
	
	public function emit($event, array $arguments = []) {
		if (isset($this->listeners[$event])) {
			foreach ($this->listeners[$event] as $listener) {
				$listener(...$arguments);
			}
		}
		if (isset($this->onceListeners[$event])) {
			$keys = array_keys($this->onceListeners[$event]);
			foreach ($keys as $key) {
				$listener = $this->onceListeners[$event][$key];
				$listener(...$arguments);
				unset($this->onceListeners[$event][$key]);
			}
			if (count($this->onceListeners[$event]) === 0) {
				unset($this->onceListeners[$event]);
			}
		}
	}
}


// stream_select() based event-loop.
class EventLoop {
	const MICROSECONDS_PER_SECOND = 1000000;
	
	private $futureTickQueue;
	private $timers;
	private $readStreams = [];
	private $readListeners = [];
	private $writeStreams = [];
	private $writeListeners = [];
	private $running;
	
	public function __construct() {
		$this->futureTickQueue = new TickQueue();
		$this->timers = new Timers();
	}
	
	public function addReadStream($stream, callable $listener) {
		$key = (int) $stream;
		if (!isset($this->readStreams[$key])) {
			$this->readStreams[$key] = $stream;
			$this->readListeners[$key] = $listener;
		}
	}
	
	public function addWriteStream($stream, callable $listener) {
		$key = (int) $stream;
		if (!isset($this->writeStreams[$key])) {
			$this->writeStreams[$key] = $stream;
			$this->writeListeners[$key] = $listener;
		}
	}
	
	public function removeReadStream($stream) {
		$key = (int) $stream;
		unset(
				$this->readStreams[$key],
				$this->readListeners[$key]
				);
	}
	
	public function removeWriteStream($stream) {
		$key = (int) $stream;
		unset(
				$this->writeStreams[$key],
				$this->writeListeners[$key]
				);
	}
	
	public function removeStream($stream) {
		$this->removeReadStream($stream);
		$this->removeWriteStream($stream);
	}
	
	public function addTimer($interval, callable $callback, $periodic=false) {
		$timer = new Timer($interval, $callback, (bool)$periodic);
		$this->timers->add($timer);
		return $timer;
	}
	
	public function cancelTimer(Timer $timer) {
		$this->timers->cancel($timer);
	}
	
	public function isTimerActive(Timer $timer) {
		return $this->timers->contains($timer);
	}
	
	public function futureTick(callable $listener) {
		$this->futureTickQueue->add($listener);
	}
	
	public function runOnce() {
		$this->run(true);
	}
	
	public function run($exit_on_empty=false) {
		$this->running = true;
		while ($this->running) {
			$this->futureTickQueue->tick();
			$this->timers->tick();
			
			// tick queue has pending callbacks ...
			if (!$this->running || !$this->futureTickQueue->isEmpty()) {
				$timeout = 0;
				
				// There is a pending timer, only block until it is due ...
			} elseif ($scheduledAt = $this->timers->getFirst()) {
				$timeout = $scheduledAt - $this->timers->getTime();
				if ($timeout < 0) {
					$timeout = 0;
				} else {
					$timeout = round($timeout * self::MICROSECONDS_PER_SECOND);
				}
				
				// The only possible event is stream activity, so wait forever ...
			} elseif ($this->readStreams || $this->writeStreams) {
				$timeout = null;
			} else {
				if ($exit_on_empty){
					break;
				}
				$timeout = round(0.01 * self::MICROSECONDS_PER_SECOND);
			}
			
			$this->waitForStreamActivity($timeout);
		}
	}
	
	public function stop() {
		$this->running = false;
	}
	
	private function waitForStreamActivity($timeout) {
		$read  = $this->readStreams;
		$write = $this->writeStreams;
		
		$available = $this->streamSelect($read, $write, $timeout);
		if ($available === false) {
			return;
		}
		
		foreach ($read as $stream) {
			$key = (int) $stream;
			
			if (isset($this->readListeners[$key])) {
				call_user_func($this->readListeners[$key], $stream, $this);
			}
		}
		
		foreach ($write as $stream) {
			$key = (int) $stream;
			
			if (isset($this->writeListeners[$key])) {
				call_user_func($this->writeListeners[$key], $stream, $this);
			}
		}
	}
	
	protected function streamSelect(array &$read, array &$write, $timeout) {
		if ($read || $write) {
			$except = null;
			// suppress warnings that occur, when stream_select is interrupted by a signal
			return @stream_select($read, $write, $except, $timeout === null ? null : 0, $timeout);
		}
		
		$timeout && usleep($timeout);
		return 0;
	}
}


final class TickQueue {
	private $queue;
	
	public function __construct() {
		$this->queue = new SplQueue();
	}
	
	public function add(callable $listener) {
		$this->queue->enqueue($listener);
	}
	
	public function tick() {
		$count = $this->queue->count();
		while ($count--) {
			call_user_func( $this->queue->dequeue() );
		}
	}
	
	public function isEmpty() {
		return $this->queue->isEmpty();
	}
}

final class Timer {
	const MIN_INTERVAL = 0.000001;
	
	private $interval;
	private $callback;
	private $periodic;
	
	public function __construct($interval, callable $callback, $periodic = false) {
		if ($interval < self::MIN_INTERVAL) {
			$interval = self::MIN_INTERVAL;
		}
		$this->interval = (float) $interval;
		$this->callback = $callback;
		$this->periodic = (bool) $periodic;
	}
	
	public function getInterval() {
		return $this->interval;
	}
	
	public function getCallback() {
		return $this->callback;
	}
	
	public function isPeriodic() {
		return $this->periodic;
	}
}


final class Timers {
	private $time;
	private $timers;
	private $scheduler;
	
	public function __construct() {
		$this->timers = new SplObjectStorage();
		$this->scheduler = new SplPriorityQueue();
	}
	
	public function updateTime() {
		return $this->time = microtime(true);
	}
	
	public function getTime() {
		return $this->time ?: $this->updateTime();
	}
	
	public function add(Timer $timer) {
		$interval = $timer->getInterval();
		$scheduledAt = $interval + microtime(true);
		
		$this->timers->attach($timer, $scheduledAt);
		$this->scheduler->insert($timer, -$scheduledAt);
	}
	
	public function contains(Timer $timer) {
		return $this->timers->contains($timer);
	}
	
	public function cancel(Timer $timer) {
		$this->timers->detach($timer);
	}
	
	public function getFirst() {
		while ($this->scheduler->count()) {
			$timer = $this->scheduler->top();
			
			if ($this->timers->contains($timer)) {
				return $this->timers[$timer];
			}
			
			$this->scheduler->extract();
		}
		
		return null;
	}
	
	public function isEmpty() {
		return count($this->timers) === 0;
	}
	
	public function tick() {
		$time = $this->updateTime();
		$timers = $this->timers;
		$scheduler = $this->scheduler;
		
		while (!$scheduler->isEmpty()) {
			$timer = $scheduler->top();
			
			if (!isset($timers[$timer])) {
				$scheduler->extract();
				$timers->detach($timer);
				
				continue;
			}
			
			if ($timers[$timer] >= $time) {
				break;
			}
			
			$scheduler->extract();
			call_user_func($timer->getCallback(), $timer);
			
			if ($timer->isPeriodic() && isset($timers[$timer])) {
				$timers[$timer] = $scheduledAt = $timer->getInterval() + $time;
				$scheduler->insert($timer, -$scheduledAt);
			} else {
				$timers->detach($timer);
			}
		}
	}
}


class Stream {
	use EventEmitter;
	
	private $stream;
	private $loop;
	private $softLimit;
	private $readBufferSize;
	
	private $writable = true;
	private $readable = true;
	private $closed = false;
	
	private $data = '';
	
	public function __construct($stream, EventLoop $loop, $writeBufferSoftLimit = null, $readChunkSize = null) {
		if (!is_resource($stream) || get_resource_type($stream) !== "stream") {
			throw new \InvalidArgumentException('Stream required');
		}
		
		$meta = stream_get_meta_data($stream);
		if (isset($meta['mode']) && $meta['mode'] !== '' && strpos($meta['mode'], '+') === false) {
			throw new InvalidArgumentException('Given stream resource is not opened in read and write mode');
		}
		
		if (stream_set_blocking($stream, 0) !== true) {
			throw new \RuntimeException('Unable to set non-blocking mode');
		}
		
		$this->stream = $stream;
		$this->loop = $loop;
		$this->softLimit = ($writeBufferSoftLimit === null) ? 65536 : (int)$writeBufferSoftLimit;
		$this->readBufferSize= ($readChunkSize === null) ? 65536 : (int)$readChunkSize;
		
		$this->resume();
	} 
	
	public function isActive(){
		return !$this->closed;
	}
	
	public function pause() {
		$this->loop->removeReadStream($this->stream);
	}
	
	public function resume() {
		if ($this->readable) {
			$this->loop->addReadStream($this->stream, array($this, 'handleRead')); 
		}
	}
	
	public function write($data) {
		if (!$this->writable) {
			return false;
		}
		
		$this->data .= $data;
		if ($this->data !== '') {
			$this->loop->addWriteStream($this->stream, array($this, 'handleWrite'));
		}
		
		return !isset($this->data[$this->softLimit - 1]);
	}
	
	public function end($data = null) {
		if (null !== $data) {
			$this->write($data);
		}
		
		$this->readable = false;
		$this->writable = false;
		
		// close immediately if buffer is already empty
		// otherwise wait for buffer to flush first
		if ($this->data === '') {
			$this->close();
		}
	}
	
	public function close() {
		if ($this->closed) {
			return;
		}
		
		$this->loop->removeStream($this->stream);
		
		$this->closed = true;
		$this->readable = false;
		$this->writable = false;
		$this->data = '';
		
		$this->emit('close', array($this));
		//$this->removeAllListeners();
		
		$this->handleClose();
	}
	
	public function handleClose() {
		if (is_resource($this->stream)) {
			fclose($this->stream);
		}
	}
	
	
	public function handleRead() {
		$error = null;
		set_error_handler(function ($errno, $errstr, $errfile, $errline) use (&$error) {
			$error = new ErrorException(
					$errstr,
					0,
					$errno,
					$errfile,
					$errline
					);
		});
			
			$data = stream_get_contents($this->stream, $this->readBufferSize);
			
			restore_error_handler();
			
			if ($error !== null) {
				$this->close();
				$this->emit('error', array(new RuntimeException('Unable to read from stream: ' . $error->getMessage(), 0, $error)));
				
				return;
			}
			
			if ($data !== '') {
				$this->emit('data', array($data));
			} else {
				// no data read => we reached the end and close the stream
				$this->close();
				$this->emit('error', array(new RuntimeException('Closed by remote server')) );
			}
	}
	
	
	public function handleWrite() {
		$error = null;
		set_error_handler(function ($errno, $errstr, $errfile, $errline) use (&$error) {
			$error = array(
					'message' => $errstr,
					'number' => $errno,
					'file' => $errfile,
					'line' => $errline
			);
		});
			
			$sent = fwrite($this->stream, $this->data);
			
			restore_error_handler();
			
			// Only report errors if *nothing* could be sent.
			// Any hard (permanent) error will fail to send any data at all.
			// Sending excessive amounts of data will only flush *some* data and then
			// report a temporary error (EAGAIN) which we do not raise here in order
			// to keep the stream open for further tries to write.
			// Should this turn out to be a permanent error later, it will eventually
			// send *nothing* and we can detect this.
			if ($sent === 0 || $sent === false) {
				if ($error !== null) {
					$error = new ErrorException(
							$error['message'],
							0,
							$error['number'],
							$error['file'],
							$error['line']
							);
				}
				
				$this->close();
				$this->emit('error', array(new RuntimeException('Unable to write to stream: ' . ($error !== null ? $error->getMessage() : 'Unknown error'), 0, $error)));
				
				return;
			}
			
			$exceeded = isset($this->data[$this->softLimit - 1]);
			$this->data = (string) substr($this->data, $sent);
			
			// buffer has been above limit and is now below limit
			if ($exceeded && !isset($this->data[$this->softLimit - 1])) {
				$this->emit('drain');
			}
			
			// buffer is now completely empty => stop trying to write
			if ($this->data === '') {
				$this->loop->removeWriteStream($this->stream);
				
				// buffer is end()ing and now completely empty => close buffer
				if (!$this->writable) {
					$this->close();
				}
			}
	}
}