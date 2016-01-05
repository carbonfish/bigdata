package edu.itu.bigdata.main;

import org.apache.hadoop.util.ProgramDriver;

import edu.itu.bigdata.gen.TeraGen;
import edu.itu.bigdata.sort.Sort;
import edu.itu.bigdata.validation.Validation;

public class Driver {
	public static void main(String argv[]) throws Throwable {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		pgd.addClass("gen", TeraGen.class, "Generate data for the terasort");
		pgd.addClass("sort", Sort.class, "Run the terasort");
		pgd.addClass("validate", Validation.class, "Checking results of terasort");
		exitCode = pgd.run(argv);
		System.exit(exitCode);
	}

}
