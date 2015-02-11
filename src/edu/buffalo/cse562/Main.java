package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Main {

	public static void main(String[] args) {
		System.out.print("We, the members of our team, agree that we will not submit any code "
				+ "that we have not written ourselves, share our code with anyone outside of "
				+ "our group, or use code that we have not written ourselves as a reference.");
		List<File> sqlFiles = new ArrayList<File>();
		File dataFolder = null;
		for (int i = 0; i < args.length; i++) {
			if (args[i].contains("--data")) {
				i++;
				dataFolder = new File(args[i]);
			} else
				sqlFiles.add(new File(args[i]));
		}
		if (dataFolder == null) {
			System.out.println("Data file not present");
		}
	}
}
