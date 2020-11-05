package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;
        private LR_RecordComparator recordComparator;


        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            SortOperator leftSortOperator = new SortOperator(SortMergeOperator.this.getTransaction(), this.getLeftTableName(), new LeftRecordComparator());
            SortOperator rightSortOperator = new SortOperator(SortMergeOperator.this.getTransaction(), this.getRightTableName(), new RightRecordComparator());
            String leftTableName = leftSortOperator.sort();
            String rightTableName = rightSortOperator.sort();

            leftIterator = SortMergeOperator.this.getRecordIterator(leftTableName);
            rightIterator = SortMergeOperator.this.getRecordIterator(rightTableName);
            leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            nextRecord = null;
            rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
            marked = false;
            recordComparator = new LR_RecordComparator();

            try {
                fetchNext();
            } catch (UnsupportedOperationException e) {
                this.nextRecord = null;
            }

        }

        private void fetchNext() throws UnsupportedOperationException {
            if (leftRecord == null) throw new UnsupportedOperationException("No new record to fetch");
            nextRecord = null;
            do {
                if (!marked) {
                    if (leftRecord == null) {
                        throw new UnsupportedOperationException("No new record to fetch");
                    }
                    while (leftRecord != null && recordComparator.compare(leftRecord, rightRecord) < 0) {
                        leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                    }
                    while (rightRecord != null && recordComparator.compare(leftRecord, rightRecord) > 0) {
                        rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    }
                    marked = true;
                    rightIterator.markPrev();
                }

                if (leftRecord != null && rightRecord != null && recordComparator.compare(leftRecord, rightRecord) == 0) {
                    List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
                    List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                    leftValues.addAll(rightValues);
                    nextRecord = new Record(leftValues);
                    rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                }
                else {
                    rightIterator.reset();
                    rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                    marked = false;
                }
            } while (!hasNext());
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(proj3_part1): implement

            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(proj3_part1): implement

            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNext();
            } catch (UnsupportedOperationException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }

        private class LR_RecordComparator implements Comparator<Record> {
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
