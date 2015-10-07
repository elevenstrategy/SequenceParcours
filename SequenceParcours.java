package parcours;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class SequenceParcoursGus extends AccumulatorEvalFunc<DataBag>
{
  private final int start;
  
  private DataBag outputBag;
  private long i;
  private long count;
  private long nevents;
  private String tampon;

  public SequenceParcoursGus()
  {
    this("0");
  }

  public SequenceParcoursGus(String start)
  {
    this.start = Integer.parseInt(start);
    cleanup();
  }
  
  @Override
  public void accumulate(Tuple arg0) throws IOException
  {
    nevents=13;
    i=nevents+1;
    DataBag inputBag = (DataBag)arg0.get(0);
      Tuple t2 = TupleFactory.getInstance().newTuple();
    for (Tuple t : inputBag) {
      Tuple t1 = TupleFactory.getInstance().newTuple(t.getAll());
      tampon=t1.get(2).toString();
      if (tampon.equals("NA souscription Credit Conso")) {
        if (i <= nevents) {
          outputBag.add(t2);
          t2=TupleFactory.getInstance().newTuple();
         }
        i=0;
        t2.append(t1.get(0).toString());
        t2.append(t1.get(1).toString());
        t2.append(t1.get(2).toString());
        i++;
       }
       else if (i < nevents) {
        t2.append(tampon);
        i++;
       }
       else if (i == nevents) {
        t2.append(tampon);
        outputBag.add(t2);
        i++;
        t2=TupleFactory.getInstance().newTuple();
       }
      if (count % 1000000 == 0) {
        outputBag.spill();
        count = 0;
      }
      ;
      count++;
    }
    if (t2.size()!=0) {
      outputBag.add(t2);
    }
   }

  @Override
  public void cleanup()
  {
    this.outputBag = BagFactory.getInstance().newDefaultBag();
    this.i = this.start;
    this.count = 0;
  }

  @Override
  public DataBag getValue()
  {
    return outputBag;
  }
  
  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      if (input.size() != 1)
      {
        throw new RuntimeException("Expected input to have only a single field");
      }
      
      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG)
      {
        throw new RuntimeException("Expected a BAG as input");
      }
      
      Schema inputBagSchema = inputFieldSchema.schema;

      if (inputBagSchema.getField(0).type != DataType.TUPLE)
      {
        throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                                                 DataType.findTypeName(inputBagSchema.getField(0).type)));
      }
      
      Schema inputTupleSchema = inputBagSchema.getField(0).schema;
      
      Schema outputTupleSchema = inputTupleSchema.clone();
      outputTupleSchema.add(new Schema.FieldSchema("i", DataType.LONG));
      
      return new Schema(new Schema.FieldSchema(
            getSchemaName(this.getClass().getName().toLowerCase(), input),
            outputTupleSchema, 
            DataType.BAG));
    }
    catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }
}
