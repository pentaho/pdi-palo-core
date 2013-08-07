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
 *   Copyright 2012 De Bortoli Wines Pty Limited (Australia)
 */

package org.pentaho.di.palo.core;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * 
 * @author Pieter van der Merwe
 * @since  2 March 2012
 */
public class ConsolidationCollection extends ArrayList<ConsolidationElement>{

	private static final long serialVersionUID = 7357086218197082639L;
	private Hashtable<String, ConsolidationElement> allElements = new Hashtable<String, ConsolidationElement>();
	
	private void addToInternalTable(ConsolidationElement element){
		if (!allElements.containsKey(element.getName()))
			allElements.put(element.getName(), element);
	}
	
	@Override
	public boolean add(ConsolidationElement e) {
		addToInternalTable(e);
		return super.add(e);
	}
	
	@Override
	public void add(int index, ConsolidationElement element) {
		addToInternalTable(element);
		super.add(index, element);
	}
	
	public boolean hasConsolidationElement(String elementName){
		return allElements.containsKey(elementName);
	}
	
	public ConsolidationElement getConsolidationElement(String elementName){
		return allElements.get(elementName);
	}
}
