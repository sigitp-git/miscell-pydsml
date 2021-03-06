'use strict';
console.log('Loading function, Kinesis Firehose, transform data before saving to S3, add line break to end of data');

exports.handler = (event, context, callback) => {
    /* Process the list of records and transform them */
    
    let buff = new Buffer('\n');  
    let base64data = buff.toString('base64');
    
    const output = event.records.map((record) => ({
        /* This transformation is the "identity" transformation, the data is left intact */
        recordId: record.recordId,
        result: 'Ok',
        data: record.data + base64data,
    }));
    
    console.log(`Processing completed.  Successful records ${output.length}.`);
    callback(null, { records: output });
};
