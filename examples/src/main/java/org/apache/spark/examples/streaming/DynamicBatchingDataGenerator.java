import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Arrays;

public final class DynamicBatchingDataGenerator {
	
	private static ArrayList<String> listOfSentences = new ArrayList<String>(Arrays.asList(
		"Most automated sentiment analysis tools are shit.",
		"VADER sentiment analysis is the shit.",
		"Sentiment analysis has never been good.",
		"Sentiment analysis with VADER has never been this good.",
		"Warren Beatty has never been so entertaining.",
		"I wont say that the movie is astounding and I wouldnt claim that the movie is too banal either.",
		"I like to hate Michael Bay films, but I couldnt fault this one",
		"Its one thing to watch an Uwe Boll film, but another thing entirely to pay for it",
		"The movie was too good",
		"This movie was actually neither that funny, nor super witty.",
		"This movie doesnt care about cleverness, wit or any other kind of intelligent humor.",
		"Those who find ugly meanings in beautiful things are corrupt without being charming.",
		"There are slow and repetitive parts, BUT it has just enough spice to keep it interesting.",
		"The script is not fantastic, but the acting is decent and the cinematography is EXCELLENT!",
		"Roger Dodger is one of the most compelling variations on this theme.",
		"Roger Dodger is one of the least compelling variations on this theme.",
		"Roger Dodger is at least compelling as a variation on the theme.",
		"they fall in love with the product",
		"but then it breaks",
		"usually around the time the 90 day warranty expires",
		"the twin towers collapsed today",
		"However, Mr. Carter solemnly argues, his client carried out the kidnapping under orders and in the least offensive way possible."));

	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Usage: DynamicBatchingDataGenerator <port>");
			System.exit(1);
		}

		Integer port = Integer.parseInt(args[0].trim());

		ServerSocket dataGeneratorSocket = null;
		try {
			dataGeneratorSocket = new ServerSocket(port);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		Socket dataAcceptorSocket = null;
		try {
			dataAcceptorSocket = dataGeneratorSocket.accept();
			DataOutputStream dataOutStream = new DataOutputStream(dataAcceptorSocket.getOutputStream());
			while(true) {
				for(String s: listOfSentences) {
					dataOutStream.writeUTF(s);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

}