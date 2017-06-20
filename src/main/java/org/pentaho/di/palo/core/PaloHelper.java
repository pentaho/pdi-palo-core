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
*   Portions Copyright 2008 Stratebi Business Solutions, S.L.
*   Portions Copyright 2011 - 2012 De Bortoli Wines Pty Limited (Australia)
*   Portions Copyright 2010 - 2013 Pentaho Corporation
*/

package org.pentaho.di.palo.core;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Level;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseFactoryInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;

import com.jedox.palojlib.interfaces.ICell;
import com.jedox.palojlib.interfaces.ICellsExporter;
import com.jedox.palojlib.interfaces.IConnection;
import com.jedox.palojlib.interfaces.IConsolidation;
import com.jedox.palojlib.interfaces.ICube;
import com.jedox.palojlib.interfaces.ICube.CellsExportType;
import com.jedox.palojlib.interfaces.ICube.SplashMode;
import com.jedox.palojlib.interfaces.IDatabase;
import com.jedox.palojlib.interfaces.IDimension;
import com.jedox.palojlib.interfaces.ICube.CubeType;
import com.jedox.palojlib.interfaces.IElement;
import com.jedox.palojlib.interfaces.IElement.ElementType;
import com.jedox.palojlib.main.ConnectionConfiguration;
import com.jedox.palojlib.main.ConnectionManager;
import com.jedox.palojlib.main.Consolidation;
import com.jedox.palojlib.main.Database;
import com.jedox.palojlib.managers.LoggerManager;

public class PaloHelper implements DatabaseFactoryInterface {

	public static boolean connectingToPalo = false;
	private DatabaseMeta databaseMeta;
	private PaloCubeCache cubeCache;
	private PaloDimensionCache dimensionCache;
	private IDatabase database;
	private IConnection connection;
	final private Level paloAPILogLevel;
	final private PaloOptionCollection updateOptions = PaloHelper.getUpdateModeOptions();
	final private PaloOptionCollection splashOptions = PaloHelper.getSplasModeOptions();
	
	// Needed by the Database dialog
	public PaloHelper(){
		paloAPILogLevel = Level.OFF;
	}
	
	public PaloHelper(final DatabaseMeta databaseMeta, final LogLevel logLevel) {
		this.databaseMeta = databaseMeta;
		
		// Map Kettle log levels to PaloAPI log levels
		switch (logLevel) {
		case NOTHING:
			paloAPILogLevel = Level.OFF;
			break;
		case ERROR:
			paloAPILogLevel = Level.ERROR;
			break;
		case MINIMAL:
		case BASIC:
			paloAPILogLevel = Level.WARN;
			break;
		case DETAILED:
			paloAPILogLevel = Level.INFO;
			break;
		case DEBUG:
			paloAPILogLevel = Level.DEBUG;
			break;
		case ROWLEVEL:
			paloAPILogLevel = Level.TRACE;
			break;
		default:
			paloAPILogLevel = Level.OFF;
			break;
		}
	}

	public String getConnectionTestReport(DatabaseMeta databaseMeta) throws KettleDatabaseException {
		StringBuffer report = new StringBuffer();

		PaloHelper helper = new PaloHelper(databaseMeta, LogLevel.ERROR);
		try {
			helper.connect();

			// If the connection was successful
			//
			report.append("Connecting to PALO server [").append(databaseMeta.getName()).append("] went without a problem.").append(Const.CR);

		} catch (KettleException e) {
			report.append("Unable to connect to the PALO server: ").append(e.getMessage()).append(Const.CR);
			report.append(Const.getStackTracker(e));
		}
		finally
		{
			helper.disconnect();	
		}

		return report.toString();
	}

	/**
	 * Connect to a PALO server
	 * @throws KettleException In case something goes wrong
	 */
	public final void connect() throws KettleException {
		while (PaloHelper.connectingToPalo) { 
			try {
				Thread.sleep(100);
			} catch(Exception ex) {}
		}
		assert databaseMeta != null;
		try {

			PaloHelper.connectingToPalo = true;

			LoggerManager.getInstance().setLevel(paloAPILogLevel);
			
			ConnectionConfiguration connConfig = new ConnectionConfiguration();
			connConfig.setHost(databaseMeta.environmentSubstitute(databaseMeta.getHostname()));
			connConfig.setPort(databaseMeta.environmentSubstitute(databaseMeta.getDatabasePortNumberString()));
			connConfig.setUsername(databaseMeta.environmentSubstitute(databaseMeta.getUsername()));
			connConfig.setPassword(databaseMeta.environmentSubstitute(databaseMeta.getPassword()));
			connConfig.setTimeout(30000);
			connection = ConnectionManager.getInstance().getConnection(connConfig);
			connection.open();
			database = connection.getDatabaseByName(databaseMeta.environmentSubstitute(databaseMeta.getDatabaseName()));

			PaloHelper.connectingToPalo = false;

			if (database == null) {
				throw new KettleException("The specified database with name '" 
						+ databaseMeta.environmentSubstitute(databaseMeta.getDatabaseName())
						+ "' could not be found");
			}
		} catch (Exception e) {
			PaloHelper.connectingToPalo = false;

			throw new KettleException("Unexpected error while connecting to "
					+ "the Palo server: "+e.getMessage(), e);
		}
	}

	/**
	 * @return a list of the names of the defined dimensions in the Palo 
	 * server database
	 * This is done 2 times for ordinary dimensions and then all others.
	 */
	public final List < String > getDimensionsNames() {
		assert database != null;
		List < String > names = new ArrayList < String >();

		for (IDimension dimension : database.getDimensions()) {

			if (!dimension.getName().startsWith("#")) {
				names.add(dimension.getName());
			}
		}

		for (IDimension dimension : database.getDimensions()) {

			if (dimension.getName().startsWith("#")) {
				names.add(dimension.getName());
			}
		}

		return names;
	}

	/**
	 * @return a list of the names of the defined cubes in the Palo server 
	 * database
	 * This is done 2 times for ordinary cubes and then all others.
	 */
	public final List < String > getCubesNames() {
		assert database != null;
		List < String > names = new ArrayList < String >();

		for (ICube cube : database.getCubes()) {
			if (cube.getType() == CubeType.CUBE_NORMAL){
				names.add(cube.getName());
			}
		}

		for (ICube cube : database.getCubes()) {
			if (cube.getType() != CubeType.CUBE_NORMAL) {
				names.add(cube.getName());
			}
		}

		return names;
	}


	public final List < String > getCubeDimensions(String cubeName) {
		assert database != null;
		List < String > cubeDimensions = new ArrayList < String >();

		ICube cube = database.getCubeByName(cubeName);
		IDimension[] dimensions = cube.getDimensions();
		for (int i = 0; i < dimensions.length; i++) {
			cubeDimensions.add(dimensions[i].getName());
		}
		return cubeDimensions;
	}

	public final RowMetaInterface getCellRowMeta(String cubeName, 
			List < DimensionField > fields, DimensionField cubeMeasure) throws KettleException {
		ICube cube = database.getCubeByName(cubeName);
		RowMetaInterface rowMeta = new RowMeta();

		IDimension[] dimensions = cube.getDimensions();
		for (int i = 0; i < dimensions.length; i++) {
			DimensionField df = null;
			for (DimensionField d : fields) {
				if(d.getDimensionName().equals(dimensions[i].getName())) {
					df = d;
					break;
				}
			}
			if (df == null) 
				throw new KettleException ("Dimension "+dimensions[i].getName()+" not found on  fields definition");
			if(df.getFieldType().equals("String"))
				rowMeta.addValueMeta(new ValueMeta(df.getFieldName(), ValueMetaInterface.TYPE_STRING));
			else {
				if(df.getFieldType().equals("Number")) 
					rowMeta.addValueMeta(new ValueMeta(df.getFieldName(), ValueMetaInterface.TYPE_NUMBER));
				else
					throw new KettleException("Only String and Number Types are acepted dimension fields");
			}

		}

		if (cubeMeasure == null) 
			throw new KettleException ("Measure field not defined.");
		if(cubeMeasure.getFieldType().equals("String"))
			rowMeta.addValueMeta(new ValueMeta(cubeMeasure.getFieldName(), ValueMetaInterface.TYPE_STRING));
		else {
			if(cubeMeasure.getFieldType().equals("Number")) 
				rowMeta.addValueMeta(new ValueMeta(cubeMeasure.getFieldName(), ValueMetaInterface.TYPE_NUMBER));
			else
				throw new KettleException("Only String and Number Types are acepted Measure field");
		}


		return rowMeta;
	}

	public final RowMetaInterface getDimensionRowMeta(String dimensionName, List < PaloDimensionLevel > levels, boolean baseElementsOnly) 
			throws KettleException {

		IDimension dimension = database.getDimensionByName(dimensionName);

		RowMetaInterface rowMeta = new RowMeta();

		if (baseElementsOnly && levels.size() != 1)
			throw new KettleException("Base elements should only have one level defined.");
		else if (baseElementsOnly == false && dimension.getDimensionInfo().getMaximumLevel() + 1 != levels.size())
			throw new KettleException("Levels of the dimension differ from defined levels");

		for ( int i = 0; i < levels.size(); i++) {
			String fieldName = levels.get(i).getFieldName();
			if (fieldName == null || fieldName == "")
				fieldName = dimensionName;
			int type = -1;
			if(levels.get(i).getFieldType().equals("String"))
				type = ValueMetaInterface.TYPE_STRING;
			else {
				if (levels.get(i).getFieldType().equals("Number"))
					type = ValueMetaInterface.TYPE_NUMBER;
				else
					throw new KettleException("Only String and Number Types are acepted dimension fields");
			}
			rowMeta.addValueMeta(new ValueMeta(fieldName, type));
		}
		return rowMeta;

	}

	public final List <PaloDimensionLevel> getDimensionLevels(String dimensionName) throws KettleException {
		List < PaloDimensionLevel > levels = new ArrayList< PaloDimensionLevel >();
		IDimension dimension = database.getDimensionByName(dimensionName);
		if (dimension == null)
			throw new KettleException("Dimension "+dimensionName+" does not exist");

		for ( int i = 0; i <= dimension.getDimensionInfo().getMaximumLevel(); i++) {
			levels.add(new PaloDimensionLevel("Level "+i,i,"",""));            
		}
		return levels;
	}

	public final void getDimensionRows(String dimensionName, RowMetaInterface rowMeta, boolean baseElementsOnly, final Listener listener) throws KettleException {
		assert database != null;
		
		final IDimension dimension = database.getDimensionByName(dimensionName);
		if (dimension == null) 
			throw new KettleException("Unable to find dimension '" + dimensionName + "' in the Palo database");

		if (baseElementsOnly){
			for (IElement element : dimension.getElements(false)){
				if (element.getType() == ElementType.ELEMENT_CONSOLIDATED)
					continue;

				Object[] row = {element.getName()};
				listener.oneMoreElement(row);
			}
		}
		else{

			// We take the maximum depth of the dimension?
			// That's the number of fields we are going to have
			// + the name of the Dimension itself (to make sure we have it all :-))
			// + the attributes / elements themselves 
			//
			final int rowSize = dimension.getDimensionInfo().getMaximumLevel() + 1;

			// Loop over the elements...
			//
			final IElement[] elementsTree = dimension.getRootElements(false);

			final Object[] row = new Object[rowSize];

			listener.prepareElements(elementsTree.length);
			for (IElement node : elementsTree) {
				// To debug
				// this.showChildren(node);
				this.assembleDimensionRows(row, rowSize, dimension, node, 0, rowMeta, listener);
			}
		}
	}

	/**
	 * Disconnect from the Palo server
	 */
	public final void disconnect() {
		assert connection != null;
		if(this.connection != null){
			try{
				connection.close();
			}
			catch(Exception e){}
		}
	}

	/**
	 * @return the databaseMeta
	 */
	public final DatabaseMeta getDatabaseMeta() {
		return databaseMeta;
	}

	/**
	 * @param databaseMeta the databaseMeta to set
	 */
	public final void setDatabaseMeta(final DatabaseMeta databaseMeta) {
		this.databaseMeta = databaseMeta;
	}

	/**
	 * @return the database
	 */
	public final IDatabase getDatabase() {
		return database;
	}

	/**
	 * @param database the database to set
	 */
	public final void setDatabase(final Database database) {
		this.database = database;
	}

	public final void manageDimension(String dimensionName, boolean createIfNotExists, boolean clearDimension, boolean clearConsolidations, boolean recreateDimension) throws KettleException{
		// if the dimension does not exist we create it
		IDimension dim = database.getDimensionByName(dimensionName);
		if (dim == null) {
			if(createIfNotExists) {
				dim = database.addDimension(dimensionName);
			} else 
				throw new KettleException("The dimension "+dimensionName + " does not exist.");
		}

		if (clearConsolidations){
			ArrayList<IElement> toDeleteArr = new ArrayList<IElement>();

			for (IElement e : dim.getElements(false))
				if (e.getType() == ElementType.ELEMENT_CONSOLIDATED)
					toDeleteArr.add(e);

			if (toDeleteArr.size() > 0)
				dim.removeElements(toDeleteArr.toArray(new IElement [toDeleteArr.size()]));
		}

		if (recreateDimension){
			database.removeDimension(dim);
			database.addDimension(dimensionName);
		}	

		// Takes ridiculously long for big dimensions.
		if(clearDimension) {
			IElement [] elem = database.getDimensionByName(dimensionName).getElements(false);
			database.getDimensionByName(dimensionName).removeElements(elem);
		}
	}
	
	public final void addDimensionElements(ArrayList<String> elements, String elementType) throws Exception {
		
		IElement.ElementType paloElementType = (elementType.equals("Numeric") ? IElement.ElementType.ELEMENT_NUMERIC : IElement.ElementType.ELEMENT_STRING);
		dimensionCache.createElements(elements, paloElementType, false);
		
	}

	public final void addDimensionConsolidations(String dimensionName, ConsolidationCollection consolidationCol) throws Exception {
		
		dimensionCache.getDimension().updateConsolidations(getConsolidations(dimensionCache, consolidationCol));
		
	}		

	public final void removeDimension(String dimensionName) {
		IDimension dim = database.getDimensionByName(dimensionName);
		database.removeDimension(dim);
	}



	public final int removeCube(String cubeName) {
		ICube cube = database.getCubeByName(cubeName);
		if (cube == null)
			return 0;
		database.removeCube(cube);
		return 1;
	}

	public final void createCube(String cubeName, String[] dimensionsNames) {
		// If cube exist exit
		ICube cube = database.getCubeByName(cubeName);
		if (cube != null)
			return;

		IDimension[] dims = new IDimension[dimensionsNames.length];
		for (int i = 0; i < dimensionsNames.length; i++) {
			dims[i] = database.getDimensionByName(dimensionsNames[i]);
		}
		database.addCube(cubeName, dims);
	}


	public final void clearCube(final String cubeName) throws Exception {
		ICube cube = database.getCubeByName(cubeName);
		if(cube == null)
			throw new Exception("The cube "+cubeName+" does not exist.");
		cube.clear();
	}

	public final void addCells(List < Object[] > cells, String addCode, String splashCode) throws Exception {
		if (cubeCache == null)
			throw new Exception("Cube cache hasn't been initialized");
		
		boolean addValues = (Boolean) updateOptions.getValue(addCode);
		SplashMode splashMode = (SplashMode) splashOptions.getValue(splashCode);

		int dimensionCount = cells.get(0).length - 1;
		IElement [][] dimensionRows = new IElement [cells.size()][dimensionCount];
		Object [] dataRows = new Object [cells.size()];

		ICube cube = cubeCache.getCube();

		IDimension[] dimensions = cubeCache.getDimensions();
		if (dimensions.length != dimensionCount)
			throw new Exception("The cube does not have so many dimensions");

		/* For each row, populate the dimension and data rows */
		for (int i = 0; i < cells.size(); i++) {
			String[] elementNames = new String[dimensionCount];

			/* Build dimension Array */
			for (int j = 0; j < dimensionCount; j++) {
				IElement element = null;
				Object elementNameObj = cells.get(i)[j];

				if (elementNameObj == null){
					String row = "";
					for (int k = 0; k < dimensionCount; k++)
						row += "[" + (cells.get(i)[k] == null ? "NULL" : cells.get(i)[k].toString()) + "] ";
					throw new Exception("Row: ("+row+") could not be added since it contains nulls in dimension " + (j + 1) + " (" + dimensions[j].getName() + ")");
				}

				elementNames[j] = elementNameObj.toString();

				element = cubeCache.getElement(j, elementNames[j]);

				if(element == null)
					throw new Exception("The dimension element "+elementNames[j]+" does not exist in dimension "+ dimensions[j].getName());

				dimensionRows[i][j] = element;
			}

			/* Build Data Array */
			Object dataValue = cells.get(i)[dimensionCount];

			if (!(dataValue instanceof Double) && !(dataValue instanceof String))
				throw new Exception("Cell value must be a Double or String to write it to Palo.");

			dataRows[i] = dataValue;
		}

		/* Do a bulk commit with all rows passed to this procedure */
		cube.loadCells(dimensionRows,dataRows,dataRows.length, addValues, splashMode, false);

	}

	/**
	 * Gets all elements from a Palo cube and notifies given listener to
	 * process each element.
	 */
	public final void getCells(final String cubeName, final RowMetaInterface rowMeta, final Listener listener) throws KettleException {
		ICube cube = database.getCubeByName(cubeName);
		IDimension[] cubeDimensions = cube.getDimensions();
		IElement[][] cubeDimensionElements = new IElement[cubeDimensions.length][];
		int[] currentDimensionIndexes = new int[cubeDimensions.length];

		// we get all the elements of all the the dimensions 
		// related to the given cube
		for (int i = 0; i < cubeDimensions.length; i++) {
			IElement[] elements = cubeDimensions[i].getElements(false);
			// int leaves = 0;
			List < IElement > list = new ArrayList < IElement > ();
			for (int j = 0; j < elements.length; j++) {
				list.add(elements[j]);
			}
			cubeDimensionElements[i] = list.toArray(new IElement[list.size()]);

			currentDimensionIndexes[i] = 0;
		}

		CellsExportType exportType = CellsExportType.ONLY_NUMERIC;
		if(rowMeta.getValueMeta(cubeDimensions.length).isString())
			exportType = CellsExportType.ONLY_STRING;

		// Need to make these parameters on the dialog
		boolean use_rules = false;
		boolean base_only = true;
		boolean skip_empty = true;
		int batchSize = 10000;

		ICellsExporter exporter = cube.getCellsExporter(cubeDimensionElements, exportType, batchSize, use_rules, base_only, skip_empty);

		while (exporter.hasNext()){

			ICell valueCell = exporter.next();

			Object[] row = new Object[cubeDimensions.length + 1];
			for (int i = 0; i < cubeDimensions.length; i++) {
				//para cada dimension cojo el valor actual que queremos sacar
				String elementValue = valueCell.getPathNames()[i];
				if(rowMeta.getValueMeta(i).isString()) {
					row[i] = elementValue;
				} else {
					if(rowMeta.getValueMeta(i).isNumber()) {
						row[i] = Double.parseDouble(elementValue);
					} else 
						throw new KettleException("Only String and Number Types are allowed for dimension values");
				}

			}


			Object data = valueCell.getValue();

			if(rowMeta.getValueMeta(cubeDimensions.length).isString()) {
				if (data instanceof String && data.toString().equals("")) {
					//warning: no data
				} else {
					row[cubeDimensions.length] = data.toString();
					listener.oneMoreElement(row);
				} 
			} else {
				if(rowMeta.getValueMeta(cubeDimensions.length).isNumber()) {
					if (data instanceof Double) {
						row[cubeDimensions.length] = data;
						listener.oneMoreElement(row);
					} else {
						if(data instanceof String && data.toString().equals("")) {
							//no value
						} else 
							throw new KettleException("Measure field is defined as Number but data from palo is: "+ data.getClass().toString());
					}
				} else 
					throw new KettleException("Only String and Number Types are accepted for Measure Field");
			}

			if(listener.getStop()) {
				while (true) {
					if(!listener.getStop() || listener.getCancel()) {
						break;
					}
				}
			}
			if(listener.getCancel())
				break;
		} 
	}

	public final void createDatabase(String databaseName) {
		assert database != null;
		this.connection.addDatabase(databaseName);
	}


	/**
	 * Listener of PaloHelper events.
	 */
	public static interface Listener {
		/**
		 * Signal notification of new element processed.
		 */
		void oneMoreElement(final Object element);

		/**
		 * Signal to prepare the listener to receive <i>maxNumberOfElements</i>
		 * <code>oneMoreElement</code> calls.
		 */
		void prepareElements(final int maxNumberOfElements);

		boolean getStop();

		void stop();
		void cancel();
		void resume();
		boolean getCancel();
	}


	private boolean isDimension(String elementName) {
		final List < String > dims = this.getDimensionsNames();
		for (final String s : dims) {
			if (s.equals(elementName)) {
				return true;
			}
		}
		return false;
	}

	@SuppressWarnings("unused")
	private void showChildren(final IElement node, int level) {
		System.out.println("Leido un elemento: " + node.getName() + " level: " + level + " hijo de :" + node.getName());
		System.out.print("TYPE ");
		switch (node.getType()) {
		case ELEMENT_CONSOLIDATED:
			System.out.println("ELEMENT_CONSOLIDATED");
			break;
		case ELEMENT_NUMERIC:
			System.out.println("ELEMENT_NUMERIC");
			break;
		case ELEMENT_STRING:
			System.out.println("ELEMENT_STRING");
			break;
		} 
		if (this.isDimension(node.getName()))
			System.out.println(node.getName() + " IS A DIMENSION");
		for (IElement child : node.getChildren())
			System.out.println("CONSOLIDATION: CHILD: " + child.getName() + " PARENT: " + node.getName());
				System.out.println("__________________________________________________");

				for (IElement childrenNode : node.getChildren()) {
					this.showChildren(childrenNode, level + 1);
				}
	}

	private void assembleDimensionRows(
			final Object[] currentRow, final int rowSize, 
			final IDimension dimension,  
			final IElement node, final int rowIndex,
			final RowMetaInterface rowMeta,
			final Listener listener) 
					throws KettleException {

		switch(node.getType()) {
		case ELEMENT_STRING:
		case ELEMENT_NUMERIC:
		case ELEMENT_CONSOLIDATED:
			if(rowMeta.getValueMeta(rowIndex).isNumber()) {
				try {
					currentRow[rowIndex] = Double.parseDouble(node.getName());
				} catch (Exception e) {
					throw new KettleException("Failed to cast Palo Element to Number Type",e);
				}
			} else {
				if(rowMeta.getValueMeta(rowIndex).isString()) {
					currentRow[rowIndex] = node.getName();
				} else
					throw new KettleException("Invalid Metadata type for dimension level. Must be Number or String");
			}
			break;
		default:
			throw new KettleException("Invalid Dimension Element Palo Type: "+node.getType());
		}

		// More children?
		if (node.getType() == ElementType.ELEMENT_CONSOLIDATED) {
			IElement[] children = node.getChildren();
			for (int i = 0; i < children.length; i++) {
				assembleDimensionRows(currentRow, rowSize, dimension, 
						children[i], rowIndex + 1, rowMeta, listener);
			}
			/* After children has been filled, clean out the old child records otherwise shorter 
			 * sibling hierarchies will hold on to these child entries. */
			for (int i = rowIndex;i < currentRow.length; i++)
				currentRow[i] = null;
		} else {
			Object[] clonedRow = new Object[currentRow.length];
			for (int i = 0;i < currentRow.length; i++) {
				clonedRow[i] = currentRow[i];
			}
			listener.oneMoreElement(clonedRow);
		}
	}
	
	private Consolidation[] getConsolidations(PaloDimensionCache dimensionCache, ConsolidationCollection consolidationCol) throws KettleException {
		
		ArrayList<IConsolidation> consolidations = new ArrayList<IConsolidation>();
		
		// Generate an entry for every consolidation element 
		for (ConsolidationElement element : consolidationCol){
			
			try {
				String parentName = element.getName();
				IElement parentElement = dimensionCache.getElement(parentName);
				
				// The HashTable is used for speed improvements, but it doesn't keep the original sorting.  The original sorting
				// is important for dimensions that include month names etc.  We need to run it with an ArrayList in parallel
				// to keep the sorting, but use the HashTable to get quick lookups for increased speed.
				Hashtable<String, IConsolidation> newConsolidations = new Hashtable<String, IConsolidation>();
				ArrayList<IConsolidation> sortedNewConsolidations = new ArrayList<IConsolidation>();
				
				// Read current consolidations if the item existed before.
				for (IElement child : parentElement.getChildren()) {
					IConsolidation e = dimensionCache.getDimension().newConsolidation(parentElement, child, child.getWeight(parentElement));
					newConsolidations.put(child.getName(), e);
					sortedNewConsolidations.add(e);
				}
				
				boolean changed = false;
				// See if new consolidations already exist with the correct weight.  If not, add to the list
				for (int i = 0; i < element.getChildren().size(); i++) {
					String childemename = element.getChildren().get(i).getElement().getName();
					if (childemename.equals(parentName))
						continue;

					IElement childElement = dimensionCache.getElement(childemename);

					/* If the weight was changed, redo the consolidation with the correct weight */
					if (newConsolidations.containsKey(childemename) 
							&& newConsolidations.get(childemename).getWeight() != element.getChildren().get(i).getConsolidationFactor())
					{
						IConsolidation oldConsol = newConsolidations.get(childemename);
						int index = sortedNewConsolidations.indexOf(oldConsol);

						IConsolidation updatedConsol = dimensionCache.getDimension().newConsolidation(parentElement, childElement, element.getChildren().get(i).getConsolidationFactor());
						newConsolidations.remove(childemename);
						newConsolidations.put(childemename, updatedConsol);
						
						// Replace the old consolidation with updated one, keeping the order
						sortedNewConsolidations.remove(index);
						sortedNewConsolidations.add(index,updatedConsol);
						
						changed = true;
					}

					if(!newConsolidations.containsKey(childemename)){
						IConsolidation e = dimensionCache.getDimension().newConsolidation(parentElement, childElement, element.getChildren().get(i).getConsolidationFactor());
						newConsolidations.put(childemename, e);
						sortedNewConsolidations.add(e);
						
						changed = true;
					}
				}

				// Copy consolidations into a structure suitable for updateConsolidations
				// Only if the consolidations changed.  Otherwise just ignore it.
				if (changed)
					consolidations.addAll(sortedNewConsolidations);
			} catch(Exception e) {
				throw new KettleException("failed to create consolidation: "+ element.getName(),e);
			}
		}
		return consolidations.toArray(new Consolidation[0]);
	}

	public void loadCubeCache(String cubeName, boolean enableCache, boolean preloadCache) throws Exception{
		this.cubeCache = new PaloCubeCache(this, cubeName, enableCache);

		if (enableCache && preloadCache)
			this.cubeCache.loadCubeCache();
	}

	public void clearCubeCache(){
		this.cubeCache = null;
	}
	
	public void loadDimensionCache(String dimensionName, boolean enableCache, boolean preloadCache) throws Exception{
		dimensionCache = new PaloDimensionCache(database, dimensionName, enableCache);
		
		if (dimensionCache != null && preloadCache)
			dimensionCache.loadDimensionCache();
	}

	public void clearDimensionCache(){
		this.dimensionCache = null;
	}
	
	public static PaloOptionCollection getUpdateModeOptions(){
		PaloOptionCollection collection = new PaloOptionCollection();
		collection.add(new PaloOption("SET", false));
		collection.add(new PaloOption("ADD", true));
		return collection;
	}
	
	public static PaloOptionCollection getSplasModeOptions(){
		PaloOptionCollection collection = new PaloOptionCollection();
		collection.add(new PaloOption("DISABLED", SplashMode.SPLASH_NOSPLASHING));
		collection.add(new PaloOption("DEFAULT", SplashMode.SPLASH_DEFAULT));
		collection.add(new PaloOption("ADD", SplashMode.SPLASH_ADD));
		collection.add(new PaloOption("SET", SplashMode.SPLASH_SET));
		return collection;
	}
}

