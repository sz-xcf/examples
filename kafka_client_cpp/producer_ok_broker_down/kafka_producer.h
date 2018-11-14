
/**
 * kafka producer for nesun, based on librdkafka
 *
 */

#ifndef _KAFKA_PRODUCER_H_
#define _KAFKA_PRODUCER_H_

#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>

#include "rdkafkacpp.h"


///////////////////////////////////////////////////////////////////////////////////////////
// class MessageDeliveryReportCb
///////////////////////////////////////////////////////////////////////////////////////////

class MessageDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
	void dr_cb (RdKafka::Message &message);
};


///////////////////////////////////////////////////////////////////////////////////////////
// class KafkaEventCb
///////////////////////////////////////////////////////////////////////////////////////////

class KafkaEventCb : public RdKafka::EventCb {
public:
	void event_cb (RdKafka::Event &event); 
};

///////////////////////////////////////////////////////////////////////////////////////////
// class KafkaProducer
///////////////////////////////////////////////////////////////////////////////////////////

class KafkaProducer{
public:
	KafkaProducer(std::string brokers, KafkaEventCb& event_cb, MessageDeliveryReportCb& deliver_report_cb);
	~KafkaProducer();

	void set_topic(std::string topic_str);
	void send_msg(std::string msg);

	RdKafka::Producer *producer;

private:
	std::string brokers;
	int32_t partition;
	RdKafka::Conf *conf;
	RdKafka::Conf *tconf;
	RdKafka::Topic *topic;
}; 


#endif //_KAFKA_PRODUCER_H_

