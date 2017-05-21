package de.tuberlin.dima.bdapro.tpch.queries;

import de.tuberlin.dima.bdapro.tpch.Config;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class Query3 extends Query{
    private ExecutionEnvironment env;
    private String sf;

    private static final long serialVersionUID = 1L;

    String[] segments = {"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY","HOUSEHOLD"};
    String segment =  getRandomSegment();
    LocalDate date = getRandomDate();
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public Query3(ExecutionEnvironment env, String sf)
    {
        super(env,sf);
    }


    @Override
    public List<Tuple4<Long, Double, String, Long>> execute()
    {

        DataSet<Tuple4<Long,Double,Double,String>> lineitems = readLineItem();
        DataSet<Tuple4<Long,Long,String, Long>> orders = readOrder();
        DataSet<Tuple2<Long,String>> customers = readCustomer();

        customers = customers.filter(filterCustomers(segment));
        orders = orders.filter(filterOrders(this.date));
        lineitems = lineitems.filter(filterLineItems(this.date));
        List<Tuple4<Long, Double, String, Long>> out;
        // Join customers with orders and package them into a ShippingPriorityItem
        DataSet<Tuple4<Long, Double, String, Long>> customerWithOrders =
                customers.join(orders).where(0).equalTo(1)
                        .with(
                                new JoinFunction<Tuple2<Long, String>, Tuple4<Long, Long, String, Long>, Tuple4<Long, Double, String, Long>>() {
                                    @Override
                                    public Tuple4<Long, Double, String, Long> join(Tuple2<Long, String> c, Tuple4<Long, Long, String, Long> o) {
                                        return new Tuple4<Long, Double, String, Long>(o.f0, 0.0, o.f2,
                                                o.f3);
                                    }
                                });

        // Join the last join result with Lineitems
        DataSet<Tuple4<Long, Double, String, Long>> result = null;
        try {
            result = customerWithOrders.join(lineitems).where(0).equalTo(0)
                    .with(
                            new JoinFunction<Tuple4<Long, Double, String, Long>, Tuple4<Long, Double, Double, String>, Tuple4<Long, Double, String, Long>>() {
                                @Override
                                public Tuple4<Long, Double, String, Long> join(Tuple4<Long, Double, String, Long> custWithOrder, Tuple4<Long, Double, Double, String> lineItem) {
                                    custWithOrder.f1 = lineItem.f1 * (1 - lineItem.f2);
                                    return custWithOrder;
                                }
                            })
                    // Group by l_orderkey, o_orderdate and o_shippriority and compute revenue sum
                    .groupBy(0, 2, 3)
                    .aggregate(Aggregations.SUM, 1);
            out = result.map(new MapFunction<Tuple4<Long, Double, String, Long>, Tuple4<Long, Double, String, Long>>() {
                @Override
                    public Tuple4<Long, Double, String, Long> map(Tuple4<Long, Double, String, Long> value) throws Exception {
                        return keepOnlyTwoDecimals(value);
                    }
                }).collect();
            return out;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private FilterFunction<Tuple4<Long, Long, String, Long>> filterOrders(LocalDate randomDate) {
        return (FilterFunction<Tuple4<Long, Long, String, Long>>) value -> {
            LocalDate date = LocalDate.parse(value.f2, dateTimeFormatter);
            return date.isBefore(randomDate)|| date.isEqual(randomDate);

        };
    }
    private FilterFunction<Tuple4<Long,Double,Double,String>> filterLineItems(LocalDate randomDate) {
        return (FilterFunction<Tuple4<Long,Double,Double,String>>) value -> {
            LocalDate date = LocalDate.parse(value.f3, dateTimeFormatter);
            return date.isAfter(randomDate);

        };
    }

    private FilterFunction<Tuple2<Long, String>> filterCustomers(String segment) {
        return (FilterFunction<Tuple2<Long, String>>) c -> c.f1.equals(segment);
    }


    private LocalDate getRandomDate()
    {
        Random rand = new Random();
        return LocalDate.of(1995, 3, rand.nextInt((31 - 1) + 1) + 1);
    }

    public void setDate(LocalDate date)
    {
        this.date = date;
    }


    public void setSegment(String segment)
    {
        this.segment = segment;
    }

    private String getRandomSegment() {
        Random rand = new Random();
        ArrayList<String> segmentArray = new ArrayList<>();
        segmentArray.addAll(Arrays.asList(segments));
        return segmentArray.get(rand.nextInt(segmentArray.size()));
    }

    private DataSet<Tuple4<Long,Double,Double,String>> readLineItem(){
        CsvReader source = getCSVReader(Config.LINEITEM);
        return source.fieldDelimiter("|").includeFields("1000011000100000").types(Long.class, Double.class,Double.class, String.class);
    }

    private DataSet<Tuple2<Long,String>> readCustomer(){
        CsvReader source = getCSVReader(Config.CUSTOMER);
        return source.fieldDelimiter("|").includeFields("10000010").types(Long.class, String.class);
    }

    private DataSource<Tuple4<Long, Long, String, Long>> readOrder(){
        CsvReader source = getCSVReader(Config.ORDERS);
        return source.fieldDelimiter("|").includeFields("110010010").types(Long.class,Long.class,String.class, Long.class);
    }
}
