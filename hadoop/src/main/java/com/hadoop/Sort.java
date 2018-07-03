package com.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Sort extends WritableComparator{

	public Sort()
	{
		super(keyPair.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		keyPair k1 = (keyPair)a;
		keyPair k2 = (keyPair)b;
		
		//Integer.compare默认就是升序排序
		int iRet = Integer.compare(k1.getYear(), k2.getYear());
		if(iRet != 0)
		{
			return iRet;
		}
		//温度降序排序
		return Integer.compare(k2.getTemperature(), k1.getTemperature());
	}
}
