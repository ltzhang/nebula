## The current effor is to integrate kvt under src/clients/storage/kvt into the nebulagraph graph compute storage layer. We will also try to integrate kvt into the metadata service in a later stage. 

- kvt is an interface to a key-value transactional store. The API is exposed as kvt_inc.h, you should regard kvt to have all the nice properties such as scalable, high performance, ACID, durable, and so on. Do not assume any weakness unless the interface does not support it. In that case, explicitly mention it in documents. 

- In the future, we will mostly work under the src/clients/storage/ directory, so treat that as the root direcotry unless absolutely necessary (such as change build script, change execution configuration, build the project and so on). 

- we provide a kvt_mem.cpp and kvt_mem.h of a simple memory based kvt implementation so that a linked executable can run. The kvt_mem reference implements the interface with transaction support, but do not assume that all KVT stores have the weakness of the memory implementation. You can use kvt_mem by link kvt_mem.o, which can be produced with "g++ -c kvt_mem.cpp" command. 

- we integrate kvt into nebula graph (replacing the storage layer) by inserting the kvt store into the client code, so when the nebulag graph compute layer demands data, it goes through the client and directly interact with the linked kvt store instead of remotely call into a distributed storage. 



