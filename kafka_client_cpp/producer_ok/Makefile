
mydemo: mydemo.cpp kafka_producer.cpp kafka_producer.h
	g++ -std=c++11 $^ -o mydemo -I/usr/local/include/librdkafka -L. -L/usr/local/lib -lrdkafka++ -lpthread

clean:
	$(RM) mydemo
