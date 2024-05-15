package telran.aws;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONObject;
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
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

public class PopulatorHandler implements RequestHandler<KinesisEvent, String> {
	private static final PopulatorHandler INSTANCE = new PopulatorHandler();
	Pattern p = Pattern.compile("{.*}");
	AmazonDynamoDB client;
	DynamoDB dynamo;
	Table table;
	String tableName = "avg_values";

	private PopulatorHandler() {
		client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
		dynamo = new DynamoDB(client);
		table = dynamo.getTable(tableName);
	}

	public static PopulatorHandler getInstance() {
		return INSTANCE;
	}

	@Override
	public String handleRequest(KinesisEvent input, Context context) {
		input.getRecords().stream().map(r -> new String(r.getKinesis().getData().array())).map(s -> getMapFromString(s))
				.forEach(s -> putToTable(s));
		return null;
	}

	private void putToTable(Map<String, Object> map) {
		table.putItem(new PutItemSpec().withItem(Item.fromMap(map)));
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getMapFromString(String s) {
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
			res = (Map<String, Object>) parser.parse(getJsonfromString(s), containerFactory);
		} catch (ParseException e) {
			System.out.println("position: " + e.getPosition());
			System.out.println(e);
		}
		return res;
	}

	private String getJsonfromString(String s) {
		Matcher m = p.matcher("s");
		m.find();
		return m.group();
	}

}
