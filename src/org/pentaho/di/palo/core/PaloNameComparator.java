package org.pentaho.di.palo.core;

import java.util.Comparator;

public class PaloNameComparator implements Comparator<String> { 

	@Override
	public int compare(String arg0, String arg1) {
		if (arg0.startsWith("#") && !arg1.startsWith("#"))
			return 1;

		if (!arg0.startsWith("#") && arg1.startsWith("#"))
			return -1;

		return arg0.compareTo(arg1);
	}
}