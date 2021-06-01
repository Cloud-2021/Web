package edu.xidian.sselab.cloudcourse.controller;

import edu.xidian.sselab.cloudcourse.domain.Record;
import edu.xidian.sselab.cloudcourse.repository.RecordRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class RecordControllerTests {

    @Autowired
    private MockMvc mvc;

    @Autowired
    public RecordRepository recordRepository;

    @Test
    public void getRecordTest() throws Exception {
        Record record = new Record();
        record.setEid("33041100025046");
        record.setPlaceId(64);
        record.setTime((long) 1496246400);
        List<Record> recordList  = recordRepository.findAllByRecord(record);
        for (Record record1: recordList){
            System.out.println(record1.toString());
        }
    }
    
}
