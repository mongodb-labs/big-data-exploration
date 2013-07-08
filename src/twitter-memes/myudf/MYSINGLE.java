package myudf;
import java.io.IOException;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.EvalFunc;
import java.util.HashMap;


public class MYSINGLE extends EvalFunc<DataBag>{
	TupleFactory mTupleFactory = TupleFactory.getInstance();
	BagFactory mBagFactory = BagFactory.getInstance();

	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		
		try {
			
			DataBag output = mBagFactory.newDefaultBag();
            Tuple nested = (Tuple) input.get(0);
			for (Object o : nested.getAll()) {
                HashMap<String, String> db = (HashMap<String, String>) o;
				output.add(mTupleFactory.newTuple(db.get("single")));
			}
			
			return output;
			
		} catch(Exception e) {
			return null;
		}
	}
}
