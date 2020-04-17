package uk.ac.ljmu.fet.cs.cloud.examples.autoscaler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;

/**
 * An alternative implementation to the threshold based virtual infrastructure
 * Using a bigger number of constrains to decide if creating or destroying a VM for a given executable
 */
public class StepScaler extends VirtualInfrastructure {

	/**
	 * We keep track of how many times we found the last VM completely unused for an
	 * particular executable
	 */
	private final HashMap<VirtualMachine, Integer> unnecessaryHits = new HashMap<VirtualMachine, Integer>();

	/**
	 * Initialises the auto scaling mechanism
	 * 
	 * @param cloud the physical infrastructure to use to rent the VMs from
	 */
	public StepScaler(final IaaSService cloud) {
		super(cloud);
	}
	/**
	 * The auto scaling mechanism that is run regularly to determine if the virtual
	 * infrastructure needs some changes. The logic is the following:
	 * This auto scaler utilise 4 different adjustment rates, when a VM usage reaches a certain
	 * percentage, it checks what is the expected number of VMs for that utilisation, removing or adding new VMs 
	 * until the expected number of VMs is reached
	 *
	 */
	@Override
	public void tick(long fires) {
		// Regular operation, the actual "autoscaler"

		// Determining if we need to get rid of a VM:
		Iterator<String> kinds = vmSetPerKind.keySet().iterator();
		while (kinds.hasNext()) {
			String kind = kinds.next();
			ArrayList<VirtualMachine> vmset = vmSetPerKind.get(kind);
			// Determining if we need a brand new kind of VM:
			if (vmset.isEmpty()) {
				// No VM with this kind yet, we need at least one for each so let's create one
				requestVM(kind);
				continue;
			} else if (vmset.size() == 1) {
				final VirtualMachine onlyMachine = vmset.get(0);
				// We will try to not destroy the last VM from any kind
				if (onlyMachine.underProcessing.isEmpty() && onlyMachine.toBeAdded.isEmpty()) {
					// It has no ongoing computation
					Integer i = unnecessaryHits.get(onlyMachine);
					if (i == null) {
						unnecessaryHits.put(onlyMachine, 1);
					} else {
						i++;
						if (i < 30) {
							unnecessaryHits.put(onlyMachine, i);
						} else {
							// After an hour of disuse, we just drop the VM
							unnecessaryHits.remove(onlyMachine);
							destroyVM(onlyMachine);
							kinds.remove();						
						}
					}
					// We don't need to check if we need more VMs as it has no computation
					continue;
				}
				// The VM now does some stuff now so we make sure we don't try to remove it
				// prematurely
				unnecessaryHits.remove(onlyMachine);
				// Now we allow the check if we need more VMs.
			} 			
			//			System.out.println(vmset.size());
			double subHourUtilSum = 0;			
			//sum the hourly utilisation of every Vm
			for (VirtualMachine vm : vmset) {
				subHourUtilSum += getHourlyUtilisationPercForVM(vm);
			}
			//divide by the number of VMs to find the average hourly utilisation
			double averageUtilisation = subHourUtilSum / vmset.size();
			int adjustment = 0;

			//utilisation less than 40%, only one VM expected
			if(averageUtilisation <0.4) {
				adjustment =1;
				//utilisation between 40% and 60%, 2 VMs expected
			}else if(averageUtilisation >0.4 && averageUtilisation <0.6) {
				adjustment =2;
				//utilisation between 60% and 90%, 5 VMs expected
			}else if(averageUtilisation >0.6 && averageUtilisation <0.9) {
				adjustment = 5;
				//more than 90%, create new Vms until it drops
			}else if(averageUtilisation >0.9 ) {
				adjustment = vmset.size() -1;
			}
			//destroy an unused VMs if the current number of VMs is more than the expected one
			if(vmset.size()>adjustment) {
				for (int i = 0; i < vmset.size(); i++) {
					final VirtualMachine vm = vmset.get(i);
					if (vm.underProcessing.isEmpty() && vm.toBeAdded.isEmpty() && vmset.size()>adjustment) {
						destroyVM(vm);					
					}
				}
				//create a new VMs if the current number of VMs is less than the requested one
			}else if(vmset.size()<adjustment) {
				requestVM(kind);				
			}
		}			

	}
}
