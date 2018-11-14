
#include "kafka_producer.h"
#include <unistd.h>
#include <thread>

using namespace std;

void producer_poll_loop(KafkaProducer &myProducer){
	for(int i=0; i<50000; i++){
		// std::cerr << "there is/are " << myProducer.producer->outq_len() << " message(s) undelivered"<< std::endl;
		myProducer.producer->poll(500);
	}

}

int main (int argc, char **argv) {

	KafkaEventCb event_cb;
	MessageDeliveryReportCb dr_cb;

	// KafkaProducer myProducer("localhost", event_cb, dr_cb);
	KafkaProducer myProducer("192.168.52.209:8881", event_cb, dr_cb);
	myProducer.set_topic("test1");
	myProducer.send_msg("test1********aaaaa");

	// myProducer.set_topic("test1");
	myProducer.send_msg("test1********zzzzzzzzzzz");

	std::thread loop_thread(producer_poll_loop, std::ref(myProducer));


	cout << "sleep 20s now=============================================\n";
	cout << "please shut down kafka_broker\n\n";
	sleep(20);
	cout << "kafka_broker should be shutted down.\n";
	cout << "send_msg and sleep(40) =============================================\n";
	myProducer.send_msg("test1********--------message 1---------");
	sleep(1);
	
	myProducer.send_msg("test1********--------message 2---------");
	sleep(2);

	myProducer.send_msg("test1********--------message 3---------");
	myProducer.send_msg("test1********--------message 4---------");
	sleep(1);
	
	myProducer.send_msg("test1********--------message 5---------");

	sleep(40);
	cout << "\n\naft send_msg ag and sleep 40, please start up kafka\n";
	sleep(20);

	// myProducer.set_topic("test1");
	cout << "kafka_broker should be startted up.\n";
	myProducer.send_msg("test1********bbbbb");
	sleep(1);
	myProducer.send_msg("test1********ccccc");
	myProducer.send_msg("test1********ddddd");

	cout << "@@@@@@@@@@@@@@@@@@@@@@@" << endl;
	loop_thread.join();
	RdKafka::wait_destroyed(5000);

	std::cout << "==================END===================\n";


	return 0;
}
