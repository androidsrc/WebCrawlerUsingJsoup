package com.android.webcrawler;

import android.app.Activity;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.os.Bundle;
import android.os.Handler;
import android.text.TextUtils;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Toast;

public class MainActivity extends Activity implements OnClickListener {

	private LinearLayout crawlingInfo;
	private Button startButton;
	private EditText urlInputView;
	private TextView progressText;

	// WebCrawler object will be used to start crawling on root Url
	private WebCrawler crawler;
	// count variable for url crawled so far
	int crawledUrlCount;
	// state variable to check crawling status
	boolean crawlingRunning;
	// For sending message to Handler in order to stop crawling after 60000 ms
	private static final int MSG_STOP_CRAWLING = 111;
	private static final int CRAWLING_RUNNING_TIME = 60000;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_main);

		crawlingInfo = (LinearLayout) findViewById(R.id.crawlingInfo);
		startButton = (Button) findViewById(R.id.start);
		urlInputView = (EditText) findViewById(R.id.webUrl);
		progressText = (TextView) findViewById(R.id.progressText);

		crawler = new WebCrawler(this, mCallback);
	}

	/**
	 * callback for crawling events
	 */
	private WebCrawler.CrawlingCallback mCallback = new WebCrawler.CrawlingCallback() {

		@Override
		public void onPageCrawlingCompleted() {
			crawledUrlCount++;
			progressText.post(new Runnable() {

				@Override
				public void run() {
					progressText.setText(crawledUrlCount
							+ " pages crawled so far!!");

				}
			});
		}

		@Override
		public void onPageCrawlingFailed(String Url, int errorCode) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onCrawlingCompleted() {
			stopCrawling();
		}

	};

	/**
	 * Callback for handling button onclick events
	 */
	@Override
	public void onClick(View v) {
		int viewId = v.getId();
		switch (viewId) {
		case R.id.start:
			String webUrl = urlInputView.getText().toString();
			if (TextUtils.isEmpty(webUrl)) {
				Toast.makeText(getApplicationContext(), "Please input web Url",
						Toast.LENGTH_SHORT).show();
			} else {
				crawlingRunning = true;
				crawler.startCrawlerTask(webUrl, true);
				startButton.setEnabled(false);
				crawlingInfo.setVisibility(View.VISIBLE);
				// Send delayed message to handler for stopping crawling
				handler.sendEmptyMessageDelayed(MSG_STOP_CRAWLING,
						CRAWLING_RUNNING_TIME);
			}
			break;
		case R.id.stop:
			// remove any scheduled messages if user stopped crawling by
			// clicking stop button
			handler.removeMessages(MSG_STOP_CRAWLING);
			stopCrawling();
			break;
		}
	}

	private Handler handler = new Handler() {
		public void handleMessage(android.os.Message msg) {
			stopCrawling();
		};
	};

	/**
	 * API to handle post crawling events
	 */
	private void stopCrawling() {
		if (crawlingRunning) {
			crawler.stopCrawlerTasks();
			crawlingInfo.setVisibility(View.INVISIBLE);
			startButton.setEnabled(true);
			startButton.setVisibility(View.VISIBLE);
			crawlingRunning = false;
			if (crawledUrlCount > 0)
				Toast.makeText(getApplicationContext(),
						printCrawledEntriesFromDb() + "pages crawled",
						Toast.LENGTH_SHORT).show();

			crawledUrlCount = 0;
			progressText.setText("");
		}
	}

	/**
	 * API to output crawled urls in logcat
	 * 
	 * @return number of rows saved in crawling database
	 */
	protected int printCrawledEntriesFromDb() {

		int count = 0;
		CrawlerDB mCrawlerDB = new CrawlerDB(this);
		SQLiteDatabase db = mCrawlerDB.getReadableDatabase();

		Cursor mCursor = db.query(CrawlerDB.TABLE_NAME, null, null, null, null,
				null, null);
		if (mCursor != null && mCursor.getCount() > 0) {
			count = mCursor.getCount();
			mCursor.moveToFirst();
			int columnIndex = mCursor
					.getColumnIndex(CrawlerDB.COLUMNS_NAME.CRAWLED_URL);
			for (int i = 0; i < count; i++) {
				Log.d("AndroidSRC_Crawler",
						"Crawled Url " + mCursor.getString(columnIndex));
				mCursor.moveToNext();
			}
		}

		return count;
	}

}
