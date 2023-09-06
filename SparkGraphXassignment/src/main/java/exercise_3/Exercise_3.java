package exercise_3;

import org.apache.spark.graphx.GraphOps;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;
import spire.random.rng.Serial;

import java.io.Serializable;
import java.util.*;
import java.util.Collections;

public class Exercise_3 {

    static class MyVertexClass extends Tuple2<Integer, List<String>> {
        public MyVertexClass() {
            super(0, new ArrayList<String>());
        }

        public MyVertexClass(Integer num) {
            super(num, new ArrayList<String>());
        }

        public MyVertexClass(Integer num, List<String> strList) {
            super(num, strList);
        }

        public Integer getNum() {
            return _1;
        }

        public List<String> getStrList() {
            return _2;
        }

        public MyVertexClass withNum(Integer num) {
            return new MyVertexClass(num, this._2);
        }

        public MyVertexClass withStrList(List<String> strList) {
            return new MyVertexClass(this._1, strList);
        }

        @Override
        public String toString() {
            return "(" + _1.toString() + ", " + _2.toString() + ")";
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof MyVertexClass)) {
                return false;
            }
            MyVertexClass otherVertex = (MyVertexClass) other;
            return Objects.equals(this._1, otherVertex._1)
                    && Objects.equals(this._2, otherVertex._2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this._1, this._2);
        }
    }

    // Initial value for pregel execution
    static final MyVertexClass INITIAL_VALUE = new MyVertexClass(Integer.MAX_VALUE, new ArrayList<String>());

    // Nodes' Labels
    static final Map<Long, String> labels = ImmutableMap.<Long, String>builder()
            .put(1l, "A")
            .put(2l, "B")
            .put(3l, "C")
            .put(4l, "D")
            .put(5l, "E")
            .put(6l, "F")
            .build();

    private static class VProg extends AbstractFunction3<Long,MyVertexClass,MyVertexClass,MyVertexClass> implements Serializable {
        @Override
        public MyVertexClass apply(Long vertexID, MyVertexClass vertexValue, MyVertexClass message) {

            if (message == INITIAL_VALUE) {
                return vertexValue;
            } else {

                if (vertexValue.getNum() >= message.getNum()) {
                    return message;
                } else {
                    return vertexValue;
                }
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<MyVertexClass,Integer>, Iterator<Tuple2<Object,MyVertexClass>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, MyVertexClass>> apply(EdgeTriplet<MyVertexClass, Integer> triplet) {

            MyVertexClass sourceVertex = triplet.srcAttr();

            if (sourceVertex.equals(INITIAL_VALUE)) {
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,MyVertexClass>>().iterator()).asScala();
            } else {
                List<String> srcList = new ArrayList<>(sourceVertex._2.size() + 1);
                srcList.addAll(sourceVertex._2);
                srcList.add(labels.get(triplet.srcId()));
                MyVertexClass total = new MyVertexClass(sourceVertex._1 + triplet.attr(), srcList);
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,MyVertexClass>(triplet.dstId(), total)).iterator()).asScala();            }
        }
    }

    private static class merge extends AbstractFunction2<MyVertexClass,MyVertexClass,MyVertexClass> implements Serializable {
        @Override
        public MyVertexClass apply(MyVertexClass msg1, MyVertexClass msg2) {
            Integer vertex1 = msg1._1();
            Integer vertex2 = msg2._1();
            if (vertex1 <= vertex2) {
                return msg1;
            } else {
                return msg2;
            }
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {

        List<Tuple2<Object, MyVertexClass>> vertices = Lists.newArrayList(
                new Tuple2<Object, MyVertexClass>(1l, new MyVertexClass(0)),
                new Tuple2<Object, MyVertexClass>(2l, new MyVertexClass(Integer.MAX_VALUE)),
                new Tuple2<Object, MyVertexClass>(3l, new MyVertexClass(Integer.MAX_VALUE)),
                new Tuple2<Object, MyVertexClass>(4l, new MyVertexClass(Integer.MAX_VALUE)),
                new Tuple2<Object, MyVertexClass>(5l, new MyVertexClass(Integer.MAX_VALUE)),
                new Tuple2<Object, MyVertexClass>(6l, new MyVertexClass(Integer.MAX_VALUE))
        );

        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object, MyVertexClass>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<MyVertexClass, Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(), new MyVertexClass(), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(MyVertexClass.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(MyVertexClass.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));


        ops.pregel(
                        INITIAL_VALUE,
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        ClassTag$.MODULE$.apply(MyVertexClass.class))
                .vertices()
                .toJavaRDD()
                .foreach(v -> {
                    Tuple2<Object, MyVertexClass> vertex = (Tuple2<Object, MyVertexClass>) v;
                    MyVertexClass valor = (MyVertexClass) vertex._2;
                    Integer coste = valor._1;
                    List<String> camino = valor._2;
                    camino.add(labels.get((Long) vertex._1));

                    System.out.println("Minimum cost to get from " + labels.get(1l) + " to " + labels.get(vertex._1) + " is " + camino + " with cost " + coste);
                });
    }
}