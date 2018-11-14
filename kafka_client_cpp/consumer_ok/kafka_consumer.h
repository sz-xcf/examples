
#ifndef _KAFKA_CONSUMER_H_
#define _KAFKA_CONSUMER_H_

#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>

#ifndef _MSC_VER
#include <sys/time.h>
#endif

#ifdef _MSC_VER
#include <atltime.h>
#else
#include <unistd.h>
#endif

#include "rdkafkacpp.h"


static bool keep_loop = true;
static void sigterm (int sig) {
	keep_loop = false;
}


///////////////////////////////////////////////////////////////////////////////////////////
// class KafkaEventCb
///////////////////////////////////////////////////////////////////////////////////////////

class KafkaEventCb : public RdKafka::EventCb {
	public:
		void event_cb (RdKafka::Event &event); 
	private:
		void print_time ();
};


///////////////////////////////////////////////////////////////////////////////////////////
// class ConsumerRebalanceCb
///////////////////////////////////////////////////////////////////////////////////////////

class ConsumerRebalanceCb : public RdKafka::RebalanceCb {
	private:
		void part_list_print (const std::vector<RdKafka::TopicPartition*>&partitions);

	public:
		void rebalance_cb (RdKafka::KafkaConsumer *consumer,
				RdKafka::ErrorCode err,
				std::vector<RdKafka::TopicPartition*> &partitions); 
};


///////////////////////////////////////////////////////////////////////////////////////////
// class MessageConsumer
///////////////////////////////////////////////////////////////////////////////////////////

class MessageConsumer
{
public:
	MessageConsumer(std::string brokerslist, std::string groupid,
			ConsumerRebalanceCb& rebalance_cb,
			KafkaEventCb& event_cb);

	~MessageConsumer();

	// topic_list: words seperated by " ", e.g. "topic1 topic2"
	void set_topics(std::string topic_list);

	void consume_loop(bool& keep_loop);

	void set_run(bool keep_loop);

private:
	std::string brokers;
	std::string groupid;
	RdKafka::KafkaConsumer *consumer;
	bool run;

	void SplitString(const std::string& s, std::vector<std::string>& v, const std::string& c);
	
	void msg_show(RdKafka::Message* message, void* opaque); 
};

#endif //_KAFKA_CONSUMER_H_
