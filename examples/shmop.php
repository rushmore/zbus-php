<?php  

if (!function_exists('sem_get')) {
	function sem_get($key) {
		return fopen(__FILE__. $key . '.sem', 'w+');
	}
	function sem_acquire($sem_id) {
		return flock($sem_id, LOCK_EX);
	}
	function sem_release($sem_id) {
		return flock($sem_id, LOCK_UN);
	}
} 

$sem = sem_get("test");
sem_acquire($sem);
sem_release($sem);

$shm_id = shmop_open(0x12345, "c", 0644, 100);

$shm_size = shmop_size($shm_id);

$shm_bytes_written = shmop_write($shm_id, "my shared memory block", 0);

$my_string = shmop_read($shm_id, 0, $shm_size);
echo $my_string . "\n";


while(true){
	sleep(1);
}
//shmop_delete($shm_id)
shmop_close($shm_id);

?> 