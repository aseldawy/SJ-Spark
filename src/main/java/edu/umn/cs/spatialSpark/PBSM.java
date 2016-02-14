package edu.umn.cs.spatialSpark;

import java.awt.geom.Rectangle2D;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class PBSM {

    public static class RectangleID extends Rectangle2D.Double {
        public int id;

        public RectangleID(int id, double x, double y, double width, double height) {
            super(x, y, width, height);
            this.id = id;
        }

        @Override
        public String toString() {
            return String.format("%d,%f,%f,%f,%f", id, x, y, x + width, y + height);
        }
    }

    public static class Grid implements Serializable {
        public double x1, y1, x2, y2;
        public int columns, rows;

        public Grid(double x1, double y1, double x2, double y2, int columns, int rows) {
            this.x1 = x1;
            this.y1 = y1;
            this.x2 = x2;
            this.y2 = y2;
            this.columns = columns;
            this.rows = rows;
        }

        public java.awt.Rectangle overlap(Rectangle2D.Double rect) {
            int col1 = (int) (Math.floor((rect.getMinX() - x1) * columns / (x2 - x1)));
            int col2 = (int) (Math.ceil((rect.getMaxX() - x1) * columns / (x2 - x1)));
            int row1 = (int) (Math.floor((rect.getMinY() - y1) * rows / (y2 - y1)));
            int row2 = (int) (Math.ceil((rect.getMaxY() - y1) * rows / (y2 - y1)));
            return new java.awt.Rectangle(col1, row1, col2 - col1, row2 - row1);
        }

        public Rectangle2D.Double getCell(int col, int row) {
            double cx1 = x1 + col * (x2 - x1) / columns;
            double cx2 = x1 + (col + 1) * (x2 - x1) / columns;
            double cy1 = y1 + row * (y2 - y1) / rows;
            double cy2 = y1 + (row + 1) * (y2 - y1) / rows;
            return new Rectangle2D.Double(cx1, cy1, cx2 - cx1, cy2 - cy1);
        }
    }

    public static List<Tuple2<RectangleID, RectangleID>> planeSweep(List<RectangleID> d1, List<RectangleID> d2,
            Rectangle2D.Double cell, List<Tuple2<RectangleID, RectangleID>> results) {
        Comparator<RectangleID> comparator = new Comparator<RectangleID>() {
            @Override
            public int compare(RectangleID o1, RectangleID o2) {
                double dx = o1.x - o2.x;
                if (dx < 0)
                    return -1;
                if (dx > 0)
                    return 1;
                return 0;
            }

        };
        d1.sort(comparator);
        d2.sort(comparator);

        int i1 = 0, i2 = 0;
        while (i1 < d1.size() && i2 < d2.size()) {
            if (d1.get(i1).x < d2.get(i2).x) {
                // d1 is active
                int ii2 = i2;
                while (ii2 < d2.size() && d2.get(ii2).x < d1.get(i1).getMaxX()) {
                    if (d1.get(i1).intersects(d2.get(ii2))) {
                        // Duplicate avoidance
                        double refpointx = Math.max(d1.get(i1).x, d2.get(ii2).x);
                        double refpointy = Math.max(d1.get(i1).y, d2.get(ii2).y);
                        if (cell.contains(refpointx, refpointy))
                            results.add(new Tuple2<RectangleID, RectangleID>(d1.get(i1), d2.get(ii2)));
                    }
                    ii2++;
                }

                i1++;
            } else {
                // d2 is active
                int ii1 = i1;
                while (ii1 < d1.size() && d1.get(ii1).x < d2.get(i2).getMaxX()) {
                    if (d1.get(ii1).intersects(d2.get(i2))) {
                        // Duplicate avoidance
                        double refpointx = Math.max(d1.get(ii1).x, d2.get(i2).x);
                        double refpointy = Math.max(d1.get(ii1).y, d2.get(i2).y);
                        if (cell.contains(refpointx, refpointy))
                            results.add(new Tuple2<RectangleID, RectangleID>(d1.get(ii1), d2.get(i2)));
                    }
                    ii1++;
                }

                i2++;
            }
        }

        return results;
    }

    public static void main(String[] args) {

        Function<String, RectangleID> parser = s -> {
            String[] parts = s.split(",");
            int id = Integer.parseInt(parts[0]);
            double x1 = Double.parseDouble(parts[1]);
            double y1 = Double.parseDouble(parts[2]);
            double x2 = Double.parseDouble(parts[3]);
            double y2 = Double.parseDouble(parts[4]);
            return new RectangleID(id, x1, y1, x2 - x1, y2 - y1);
        };
        final Grid grid = new Grid(0, 0, 1000, 1000, 16, 16);
        PairFlatMapFunction<RectangleID, Integer, RectangleID> gridAssignment = t -> {
            java.awt.Rectangle overlaps = grid.overlap(t);
            List<Tuple2<Integer, RectangleID>> replicas = new ArrayList<>();
            for (int col = overlaps.x; col < overlaps.x + overlaps.width; col++) {
                for (int row = overlaps.y; row < overlaps.y + overlaps.height; row++) {
                    replicas.add(new Tuple2<Integer, RectangleID>(row * grid.columns + col, t));
                }
            }
            return replicas;
        };

        FlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Iterable<RectangleID>, Iterable<RectangleID>>>>, Tuple2<RectangleID, RectangleID>> gridPartitioner = t -> {
            List<Tuple2<RectangleID, RectangleID>> results = new ArrayList<>();
            while (t.hasNext()) {
                Tuple2<Integer, Tuple2<Iterable<RectangleID>, Iterable<RectangleID>>> cellInfo = t.next();
                int cellId = cellInfo._1;
                Iterable<RectangleID> firstDataset = cellInfo._2._1;
                List<RectangleID> d1 = new ArrayList<RectangleID>();
                for (RectangleID r1 : firstDataset)
                    d1.add(r1);
                Iterable<RectangleID> secondDataset = cellInfo._2._2;
                List<RectangleID> d2 = new ArrayList<RectangleID>();
                for (RectangleID r2 : secondDataset)
                    d2.add(r2);

                int col = cellId % grid.columns;
                int row = cellId / grid.columns;
                java.awt.geom.Rectangle2D.Double cell = grid.getCell(col, row);
                // TODO pass cell information to apply duplicate avoidance
                planeSweep(d1, d2, cell, results);
            }
            return results;
        };

        JavaSparkContext spark = new JavaSparkContext("local", "JavaLogQuery");
        String[] inputFiles = { "rects4.txt", "rects5.txt" };

        JavaPairRDD<Integer, RectangleID> rects1 = spark.textFile(inputFiles[0]).map(parser)
                .flatMapToPair(gridAssignment);
        JavaPairRDD<Integer, RectangleID> rects2 = spark.textFile(inputFiles[1]).map(parser)
                .flatMapToPair(gridAssignment);

        long t1 = System.currentTimeMillis();
        JavaRDD<Tuple2<RectangleID, RectangleID>> joinResults = rects1.cogroup(rects2).mapPartitions(gridPartitioner);
        joinResults.saveAsTextFile("results45_actual.txt");
        long t2 = System.currentTimeMillis();

        System.out.printf("Finished spatial join in %f seconds\n", (t2 - t1) / 1000.0);
        spark.close();
    }

}
