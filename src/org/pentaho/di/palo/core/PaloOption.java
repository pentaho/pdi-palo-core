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
 * This class is used to store Palo options to be used by dialogs.  Note that you have to generate the descriptions
 * with the appropriate translation using the code. 
 * 
 * @author Pieter van der Merwe
 * @since 12-11-2011
 */

package org.pentaho.di.palo.core;

public class PaloOption {

	private final String code;
	private final Object paloValue;
	private String description;
	
	
	public PaloOption(String code, Object paloValue){
		this.code = code;
		this.paloValue = paloValue;
	}


	public String getCode() {
		return code;
	}


	public Object getPaloValue() {
		return paloValue;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		// If the locale description wasn't found, use the code.
		if (description == null || description.equals(""))
			this.description = this.code;
		else
			this.description = description;
	}
}
