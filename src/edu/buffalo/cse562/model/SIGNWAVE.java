package edu.buffalo.cse562.model;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.StringTokenizer;
public class SIGNWAVE {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(System.out));
			int t = Integer.parseInt(br.readLine());
			while(t-->0) {
				int s,c,k;
				StringTokenizer st = new StringTokenizer(br.readLine());
				s = Integer.parseInt(st.nextToken());
				c = Integer.parseInt(st.nextToken());
				k = Integer.parseInt(st.nextToken());
				if(k==1) {
					if(c==0&&s==0) {
						pw.println("0");
					}
					else if(c==0) {
						pw.println((long)(Math.pow(2,s)+1));
					}
					else if(s==0) {
						pw.println((long)(2*(Math.pow(2, c)-1)));
					}
					else if(s>c){
						pw.println((long)Math.pow(2,s)+1);
					}
					else if(s==c) {
						pw.println((long)(Math.pow(2, s+1)+1));
					}
					else if(s<c) {
						pw.println((long)(2*(Math.pow(2, c)-1)+3));
					}
				}
				else if(c==0) {
					if(k>s) {
						pw.println("0");
					}
					else {
						pw.println((long)(Math.pow(2, s-k+1)+1));
					}
				}
				else {
					if(c==0&&s==0)
						pw.println("0");
					else if(c==0) {
						pw.println((long)(Math.pow(2, s-k+1))+1);
					}
					else if(s==0) {
						pw.println("0");
					}
					else if(s-k<c) {
						long temp = (long)Math.pow(2, s-k+1);
						temp+= (long)Math.pow(2, s-k+1);
						pw.println(temp);
					}
					else {
						pw.println((long)Math.pow(2, s-k+1));
					}
				}
			}
			pw.flush();
			pw.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

}