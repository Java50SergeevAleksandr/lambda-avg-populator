package telran.aws;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

public class PopulatorHandler implements RequestHandler<KinesisEvent, String> {
	Pattern p = Pattern.compile("{.*}");
	LambdaLogger log;
	AmazonDynamoDB client;
	DynamoDB dynamo;
	Table table;
	String tableName;

	private PopulatorHandler() {
		tableName = System.getenv("TABLE_NAME");
		setDynamoDB();
	}

	private void setDynamoDB() {
		client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
		dynamo = new DynamoDB(client);
		table = dynamo.getTable(tableName);
	}

	@Override
	public String handleRequest(KinesisEvent input, Context context) {
		log = context.getLogger();
		log.log("tableName is " + tableName);
		input.getRecords().stream().map(this::getJsonfromString).forEach(s -> putToTable(s));
		return null;
	}

	private void putToTable(String recordJson) {
		log.log(String.format("received probeData: %s", recordJson));
		Map<String, Object> map = getMap(recordJson);
		table.putItem(new PutItemSpec().withItem(Item.fromMap(map)));
		log.log(String.format("item %s has been saved to Database", map));
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getMap(String json) {
		Map<String, Object> res = null;
		JSONParser parser = new JSONParser();

		ContainerFactory containerFactory = new ContainerFactory() {

			@Override
			public Map<String, Object> createObjectContainer() {
				return new HashMap<>();
			}

			@Override
			public List<String> creatArrayContainer() {
				return null;
			}

		};
		try {
			res = (Map<String, Object>) parser.parse(json, containerFactory);
		} catch (ParseException e) {
			log.log("ERROR JSON parsing: " + "position: " + e.getPosition() + e);
		}
		log.log("Map is: " + res);
		return res;
	}

	private String getJsonfromString(KinesisEventRecord event) {
		String record = new String(event.getKinesis().getData().array());
		log.log("received string:  " + record);
		Matcher m = p.matcher(record);
		m.find();
		return m.group();
	}

}
