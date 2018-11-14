
#include "kafka_producer.h"
#include <unistd.h>
#include <thread>


void producer_poll_loop(KafkaProducer &myProducer){
	for(int i=0; i<20; i++){
		std::cerr << "there is/are " << myProducer.producer->outq_len() << " message(s) undelivered"<< std::endl;
		myProducer.producer->poll(500);
	}

}

int main (int argc, char **argv) {

	KafkaEventCb event_cb;
	MessageDeliveryReportCb dr_cb;

	KafkaProducer myProducer("localhost", event_cb, dr_cb);
	myProducer.set_topic("test3");
	myProducer.send_msg("test3********aaaaa");

	myProducer.set_topic("test2");
	myProducer.send_msg("test2********zzzzzzzzzzz");

	std::thread loop_thread(producer_poll_loop, std::ref(myProducer));


	myProducer.send_msg("test2********--------message 1---------");
	sleep(1);
	
	myProducer.send_msg("test2********--------message 2---------");
	sleep(2);

	myProducer.send_msg("test2********--------message 3---------");
	sleep(3);

	myProducer.send_msg("test2********--------message 4---------");
	sleep(1);
	
	myProducer.send_msg("test2********--------message 5---------");

	myProducer.set_topic("test3");
	myProducer.send_msg("test3********bbbbb");
	sleep(1);
	myProducer.send_msg("test3********ccccc");
	myProducer.send_msg("test3********ddddd");

	loop_thread.join();
	RdKafka::wait_destroyed(5000);

	std::cout << "==================END===================\n";


	return 0;
}
