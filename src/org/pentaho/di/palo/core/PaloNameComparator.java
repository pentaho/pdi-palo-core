/*
 *   This file is part of PaloKettlePlugin.
 *
 *   PaloKettlePlugin is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Lesser General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   PaloKettlePlugin is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License
 *   along with PaloKettlePlugin.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   Portions Copyright 2011 De Bortoli Wines Pty Limited (Australia)
 */

package org.pentaho.di.palo.core;

/**
 * This class is used to sort Palo cube/dimension names using the Collections.sort function.  System names are sorted last (names
 * starting with a #). 
 * 
 * @author Pieter van der Merwe
 * @since 10-11-2011
 */

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