#include <bits/stdc++.h>
using namespace std;

int main() {
    int n = 5000;

    long long count = 0;

    for (int a = 0; a < n; a++) {
        for (int b = 0; b < n; b++) {
            for (int c = 0; c < n; c++) {
                for (int d = 0; d < n; d++) {
                    for (int e = 0; e < n; e++) {
                        // Some dummy heavy computation
                        count += (a ^ b ^ c ^ d ^ e);
                    }
                }
            }
        }
    }

    cout << "Result: " << count << endl;
    return 0;
}