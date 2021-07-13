<?php


class KafkaProducer{

    private $server;
    private $conf;
    private $rk;

    function __construct(){
        
        $this->server = 'localhost:9092';
        $this->conf = new RdKafka\Conf();
    }

    public function setServer($server){
        $this->server = $server;
    }

    public function setConfig($param, $val){

        $this->conf->set($param, $val);
    }

    private function producer(){

        $this->rk = new RdKafka\Producer($this->conf);
        $this->rk->addBrokers($this->server);
    }

    public function sendMsg($topic, $msg){

        $this->producer();
        $topic = $this->rk->newTopic($topic);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $msg);
        $this->shutdown();
    }

    private function shutdown(){
        $timeout_ms = 1000;
        $this->rk->flush($timeout_ms);
    }
}
