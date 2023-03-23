package com.hc.batchprocessing.configuration;

import com.hc.batchprocessing.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

public class CustomerProcessor implements ItemProcessor<Customer,Customer> {
    @Override
    public Customer process(Customer customer) throws Exception {
//        if(customer.getFirstName().equals("Mahesh")){
//            return customer;
//        }else{
//            return null;
//        }
        return customer;
    }
}
