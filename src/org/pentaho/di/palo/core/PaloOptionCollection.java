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
 *   Copyright 2011 De Bortoli Wines Pty Limited (Australia)
 */

/**
 * This class is used to group Palo options
 * 
 * @author Pieter van der Merwe
 * @since 12-11-2011
 */

package org.pentaho.di.palo.core;

import java.util.ArrayList;

public class PaloOptionCollection extends ArrayList<PaloOption>{
	private static final long serialVersionUID = -4487388211011246015L;

	public String getDescription (String code){
		for (PaloOption option : this){
			if (option.getCode().equals(code)){
				return option.getDescription();
			}
		}
		
		return "NOT FOUND";
	}
	
	public String getCode (String description){
		for (PaloOption option : this){
			if (option.getDescription().equals(description)){
				return option.getCode();
			}
		}
		
		return null;
	}
	
	public Object getValue (String code){
		for (PaloOption option : this){
			if (option.getCode().equals(code)){
				return option.getPaloValue();
			}
		}
		
		return null;
	}
	
}
