/**
 * 
 */
package no.hvl.dat110.chordoperations;

import java.math.BigInteger;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.middleware.Node;
import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class ChordLookup {

	private static final Logger logger = LogManager.getLogger(ChordLookup.class);
	private Node node;
	
	public ChordLookup(Node node) {
		this.node = node;
	}
	
	public NodeInterface findSuccessor(BigInteger key) throws NotBoundException, RemoteException {
		/*
		NodeInterface succ = node.findSuccessor(key);
		BigInteger succID = succ.getNodeID();
		BigInteger nodeID = node.getNodeID();

		while (!Util.checkInterval(key, nodeID, succID)) {
			NodeInterface nodePred = findHighestPredecessor(key);
			succ = nodePred.findSuccessor(key);
			succID = succ.getNodeID();
		}

		return succ;
	}
*/
		try {
			NodeInterface succ = node.getSuccessor();

			//BigInteger succID = succ.getNodeID();

			NodeInterface stub = Util.getProcessStub(succ.getNodeName(), succ.getPort());

			if(Util.checkInterval(key, node.getNodeID().add(BigInteger.ONE), stub.getNodeID())){
				return stub;
			} else {
				return findHighestPredecessor(key).findSuccessor(key);
			}
		} catch (Exception e) {
			System.out.print(e);
			return null;
		}
		// ask this node to find the successor of key
		
		// get the successor of the node
		
		// check that key is a member of the set {nodeid+1,...,succID} i.e. (nodeid+1 <= key <= succID) using the checkInterval
		
		// if logic returns true, then return the successor
		
		// if logic returns false; call findHighestPredecessor(key)
		
		// do highest_pred.findSuccessor(key) - This is a recursive call until logic returns true
				

	}
	
	/**
	 * This method makes a remote call. Invoked from a local client
	 * @param ID BigInteger
	 * @return
	 * @throws RemoteException
	 */
	private NodeInterface findHighestPredecessor(BigInteger ID) throws RemoteException, NotBoundException {

		List<NodeInterface> fingerTableEntries = node.getFingerTable();
		for (int i = 0; i < fingerTableEntries.size(); i++) {
			NodeInterface finger = fingerTableEntries.get(i);
			if (finger != null) {
				Registry registry = LocateRegistry.getRegistry(finger.getNodeID().toString(), finger.getPort());
				NodeInterface fingerStub = (NodeInterface) registry.lookup(finger.getNodeName());
			}
			if(Util.checkInterval(ID, node.getNodeID(), finger.getNodeID())){
				return finger;
			}
		}
		// collect the entries in the finger table for this node
		
		// starting from the last entry, iterate over the finger table
		
		// for each finger, obtain a stub from the registry
		
		// check that finger is a member of the set {nodeID+1,...,ID-1} i.e. (nodeID+1 <= finger <= key-1) using the ComputeLogic
		
		// if logic returns true, then return the finger (means finger is the closest to key)
		
		return (NodeInterface) node;			
	}
	
	public void copyKeysFromSuccessor(NodeInterface succ) {
		
		Set<BigInteger> filekeys;
		try {
			// if this node and succ are the same, don't do anything
			if(succ.getNodeName().equals(node.getNodeName()))
				return;
			
			logger.info("copy file keys that are <= "+node.getNodeName()+" from successor "+ succ.getNodeName()+" to "+node.getNodeName());
			
			filekeys = new HashSet<>(succ.getNodeKeys());
			BigInteger nodeID = node.getNodeID();
			
			for(BigInteger fileID : filekeys) {

				if(fileID.compareTo(nodeID) <= 0) {
					logger.info("fileID="+fileID+" | nodeID= "+nodeID);
					node.addKey(fileID); 															// re-assign file to this successor node
					Message msg = succ.getFilesMetadata().get(fileID);				
					node.saveFileContent(msg.getNameOfFile(), fileID, msg.getBytesOfFile(), msg.isPrimaryServer()); 			// save the file in memory of the newly joined node
					succ.removeKey(fileID); 	 																				// remove the file key from the successor
					succ.getFilesMetadata().remove(fileID); 																	// also remove the saved file from memory
				}
			}
			
			logger.info("Finished copying file keys from successor "+ succ.getNodeName()+" to "+node.getNodeName());
		} catch (RemoteException e) {
			logger.error(e.getMessage());
		}
	}

	public void notify(NodeInterface pred_new) throws RemoteException {
		
		NodeInterface pred_old = node.getPredecessor();
		
		// if the predecessor is null accept the new predecessor
		if(pred_old == null) {
			node.setPredecessor(pred_new);		// accept the new predecessor
			return;
		}
		
		else if(pred_new.getNodeName().equals(node.getNodeName())) {
			node.setPredecessor(null);
			return;
		} else {
			BigInteger nodeID = node.getNodeID();
			BigInteger pred_oldID = pred_old.getNodeID();
			
			BigInteger pred_newID = pred_new.getNodeID();
			
			// check that pred_new is between pred_old and this node, accept pred_new as the new predecessor
			// check that ftsuccID is a member of the set {nodeID+1,...,ID-1}
			boolean cond = Util.checkInterval(pred_newID, pred_oldID.add(BigInteger.ONE), nodeID.add(BigInteger.ONE));
			if(cond) {		
				node.setPredecessor(pred_new);		// accept the new predecessor
			}	
		}		
	}

}
