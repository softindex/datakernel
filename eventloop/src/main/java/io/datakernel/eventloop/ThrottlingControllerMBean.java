/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.eventloop;

public interface ThrottlingControllerMBean {
	double getAvgKeysPerSecond();

	double getAvgThrottling();

	int getTargetTimeMillis();

	void setTargetTimeMillis(int targetTimeMillis);

	int getGcTimeMillis();

	void setGcTimeMillis(int gcTimeMillis);

	int getSmoothingWindow();

	void setSmoothingWindow(int smoothingWindow);

	double getThrottlingDecrease();

	void setThrottlingDecrease(double throttlingDecrease);

	long getTotalRequests();

	long getTotalRequestsThrottled();

	long getTotalProcessed();

	long getTotalTimeMillis();

	long getRounds();

	long getRoundsZeroThrottling();

	long getRoundsExceededTargetTime();

	long getInfoRoundsGc();

	float getThrottling();

	String getThrottlingStatus();

	void resetInfo();
}
