//
// Created by kane on 2021/9/5.
//
#include <iostream>
#include <thread>
#include <vector>
#include <string>

#include "fptree.h"
#include "uniform_random.hpp"

class kv_generator{
public:
    kv_generator(std::string op){
        if(op == "insert")
            sequential = true;
        else
            sequential = false;

        current_id = 0;
    }

    kv_generator(){
        current_id = 1;
    };

//    void pre_generate_KV(std::vector<std::vector<KV>> & arrKV, uint64_t start_ID, uint64_t length, bool sequential){
//        arrKV.resize(length);
//        arrKV.clear();
//        uint64_t index = start_ID;
//        uint64_t end = start_ID + length - 1;
//        for(uint64_t i =0; i < length ; ++i, ++index){
//            arrKV[i].key = sequential ? multiplicative_hash(index) : multiplicative_hash(uniformRandom.uniform_within_64(start_ID,end));
//            uint32_t pos = uniformRandom.uniform_within_32(0, sizeof(VALUE_POOL) - sizeof(uint64_t) );
//            arrKV[i].value = VALUE_POOL[pos];
//        }
//    }
    void pre_generate_Keys(std::vector<uint64_t> & arrKeys, uint64_t length, bool sequential){
        arrKeys.resize(length);
        std::cout << "Generating keys. Length" << length << std::endl;

        if(sequential){
            for(uint64_t i =0 ; i < length ; ++i, ++current_id){
                arrKeys[i] = multiplicative_hash(current_id);
            }
        }
        else{
            for(uint64_t i =0 ; i < length; ++i){
                arrKeys[i] = multiplicative_hash(uniformRandom.uniform_within_64(1,current_id-1));
            }
        }
    }

    void pre_generate_Values(std::vector<uint64_t> & arrVals, uint64_t length){
        arrVals.resize(length);

        std::cout << "Generating keys. Length" << length << std::endl;
        for(uint64_t i = 0; i < length ; ++i){
            uint32_t pos = uniformRandom.uniform_within_32(0, sizeof(VALUE_POOL) - sizeof(uint64_t) );
            arrVals[i] = VALUE_POOL[pos];
        }
    }

    void set_current_id(uint64_t t) { current_id = t; }
    uint64_t get_current_id(){ return current_id; }
    void set_seed(uint64_t seed){ uniformRandom.set_current_seed(seed); }

private:

    bool sequential;
    static thread_local uint64_t current_id;

    static thread_local foedus::assorted::UniformRandom uniformRandom;

    inline uint64_t multiplicative_hash(uint64_t x)
    {
        uint64_t A = 11400714819323198393ul;
        return A * x;
    }

    static constexpr char VALUE_POOL[] =
            "NvhE8N7yR26f4bbpMJnUKgHncH6QbsI10HyxlvYHKFiMk5nPNDbueF2xKLzteSd0NazU2APkJWXvBW2oUu8dkZnWMMu37G8TH2qm"
            "S0c8A9z41pxrC6ZU79OnfCZ06DsNXWY3U4dt1JTGQVvylBdZSlHWXC4PokCxsMdjv8xRptFMMQyHZRqMhNDnrsGKA12DEr7Zur0n"
            "tZpsyreMmPwuw7WMRnoN5wAYWtkqDwXyQlYb4RgtSc4xsonpTx2UhIUi15oJTx1CXAhmECacCQfntFnrSZt5qs1L64eeJ9Utus0N"
            "mKgEFV8qYDsNtJ21TkjCyCDhVIATkEugCw1BfNIB9AZDGiqXc0llp4rlJPl4bIG2QC4La3M1oh3yGlZTmdvN5pj1sIGkolpdoYVJ"
            "0NZM9KAo1d5sGFv9yGC7X0CTDOqyRu5c4NPktU70NbKqWNXa1kcaigIfeAuvJBs0Wso2osHzOjrbawgpfBPs1ePaWHgw7vbOcu9v"
            "Cqz1GnmdQw4mGSo4cc6tebQuKqLkQHuXa1MdRmzinBRoGQBQehqrDmmfNhcxfozcU7hOTjFAjryJ4HdSK57gOlrte5sZlvDW9rFd"
            "4OxG6WtFdZomRQPTNc4D9t7smqBR9EYDSjiAAqmIgZUiycHrlv6JQzEiexjqfGUbo8oJV6wiu7l3Jlfb94uByDxoexkMT5AjJzls"
            "er1dc9EfQz88q5Hv00g53Q3H6jcgicoY8YW5K4josd2e53ikesQi2kzqvTI9xxM5wtFexkFm8wFdMs6YmNpvNgTf37Hz204wX1Sf"
            "djFmCYEcP533LYcGB7CslEVMPYRZXHBT98XKtt8RqES7HBW65xSJRSj3qhIDUsgeu2Flo4YqS68QoE69JzyBnwmmYw6uulVLVIAe"
            "iLl49oUhEiEjem8RrHPpEvrUoLDWwMdh14MfxwmEQbtGnUHEpRktUB6b7JTJN8OHBlLrvr71TkRK728ZgRv32rMZJ46O17qHTYc4"
            "AepNCGbpTII0J05OYiush6hiDo6H5pVHVUWy3nm7BBrBzEHVOCBMHNniw4CIzfavGLaUfgjlBg0D4JBmYmkg0A4maCXsE9QTnGbA"
            "fQErGZkdMnRxXJ5EJ627e7zuFuVtazb0L65B3nU5R9tyUl2bTZiDcakK9evrTXoTkbkGjkCOiMSThGFScb6Lsgvl5wNCzlUZCxof"
            "jYQCLusRkXEm0CNVuifTnytctwLfKjwob4hJ0WxlQN9FV9Mm9zT61EQ8zEMrqr6hf7XMqhcQR7DWAaf1fM4oNLIA7ZdKaspUaU6h"
            "oP2w3t3MktVaBp6MgS6Apbkb7EsihETHHqKFkKMCkYBbKfgsq7Jy49T1Wx2UJsD3XX03kVBbqRWmryYoMIqiCTCTqa0jIKzqQEnN";
};

class benchmark_opt{
private:
    std::vector<uint64_t> op_num;
public:
    benchmark_opt(){
        op_num.resize(4);
    }

    void set_opt(int idx, uint64_t num){ op_num[idx]=num;}
    uint64_t num_insert() {return op_num[0];}
    uint64_t num_search() {return op_num[1];}
    uint64_t num_update() {return op_num[2];}
    uint64_t num_remove() {return op_num[3];}
};


thread_local foedus::assorted::UniformRandom kv_generator::uniformRandom;
thread_local uint64_t kv_generator::current_id;

void thread_insert(FPtree & tree, std::vector<uint64_t> arrKeys, std::vector<uint64_t> arrVals, uint64_t num_op , uint16_t num_thread ,uint64_t id){
    uint64_t workload = num_op / num_thread , stop;
    if (id == num_thread - 1)	// last thread, load all keys left
        stop = num_op;
    else	// just normal workload
        stop = (id + 1) * workload;

    for (uint64_t i = id * workload; i < stop; ++i)
        if (!tree.insert(KV(arrKeys[i],arrVals[i])))
        {
            printf("Insert failed! Key: %llu Value: %llu\n", arrKeys[i],arrVals[i]);
            exit(1);
        }
}

void thread_read(FPtree & tree, std::vector<uint64_t> arrKeys, uint64_t num_op , uint16_t num_thread ,uint64_t id){
    uint64_t workload = num_op / num_thread , stop;
    if (id == num_thread - 1)	// last thread, load all keys left
        stop = num_op;
    else	// just normal workload
        stop = (id + 1) * workload;

    for(uint64_t i = id*workload; i < stop ; ++i){
        if(!tree.find(arrKeys[i])){
            printf("Read failed! Key: %llu\n",arrKeys[i]);
        }
    }
}

int main(){
    benchmark_opt opt;
    uint64_t num = 0;
    uint16_t t_num = 0;
    char tmp;
    std::cout << "Insert:y/n" << std::endl;
    std::cin >> tmp;
    if(tmp == 'y'){
        std::cin >> num;
        opt.set_opt(0,num);
        std::cout << "Insert num:" << opt.num_insert() << std::endl;
    }

    std::cout << "Search:y/n" << std::endl;
    std::cin >> tmp;
    if(tmp == 'y'){
        std::cin >> num;
        opt.set_opt(1,num);
        std::cout << "Search num:" << opt.num_search() << std::endl;
    }

    std::cout << "Update:y/n" << std::endl;
    std::cin >> tmp;
    if(tmp == 'y'){
        std::cin >> num;
        opt.set_opt(2,num);
        std::cout << "Update num:" << opt.num_update() << std::endl;
    }

    std::cout << "Remove:y/n" << std::endl;
    std::cin >> tmp;
    if(tmp == 'y'){
        std::cin >> num;
        opt.set_opt(3,num);
        std::cout << "Remove num:" << opt.num_remove() << std::endl;
    }

    std::cout << "thread num" << std::endl;
    std::cin >> t_num;

    FPtree fptree;
    std::vector<std::thread> workers(t_num);


    kv_generator generator;

    // Insert

    std::vector<uint64_t> insert_arrKeys(opt.num_insert());
    std::vector<uint64_t> insert_arrVals(opt.num_insert());

    uint64_t workload = opt.num_insert() / t_num ;

    generator.pre_generate_Keys( insert_arrKeys, opt.num_insert(), true);
    generator.pre_generate_Values(insert_arrVals,opt.num_insert());

    auto start = std::chrono::high_resolution_clock::now();
    for (uint64_t i = 0; i < t_num; ++i)
        workers[i] = std::thread(thread_insert, std::ref(fptree), std::ref(insert_arrKeys), std::ref(insert_arrVals), opt.num_insert(), t_num, i);
    for (uint64_t i = 0; i < t_num; i++)
        workers[i].join();
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
    std::cout << "Insert-" << opt.num_insert() << ":" << duration.count() << "milliseconds" << std::endl;
    std::vector<uint64_t>().swap(insert_arrKeys);
    std::vector<uint64_t>().swap(insert_arrVals);


    // Read

    std::vector<uint64_t> read_arrKeys(opt.num_search());
    generator.pre_generate_Keys(insert_arrKeys, opt.num_search(), false);

    start = std::chrono::high_resolution_clock::now();
    for (uint64_t i = 0; i < t_num; ++i)
        workers[i] = std::thread(thread_read, std::ref(fptree), std::ref(read_arrKeys) ,opt.num_search(), t_num, i);
    for (uint64_t i = 0; i < t_num; i++)
        workers[i].join();
    end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
    std::cout << "Insert-" << opt.num_insert() << ":" << duration.count() << "milliseconds" << std::endl;
    std::vector<uint64_t>().swap(read_arrKeys);

}

