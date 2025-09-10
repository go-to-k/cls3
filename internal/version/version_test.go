package version

import (
	"testing"
)

/*
	Test Cases
*/

func Test_IsDebug(t *testing.T) {
	type args struct {
		Version  string
		Revision string
	}

	cases := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "true if both Version and Revision are empty",
			args: args{
				Version:  "",
				Revision: "",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "true if Version is empty and Revision is not empty",
			args: args{
				Version:  "",
				Revision: "abcde",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "false if Revision is not empty and Revision is empty",
			args: args{
				Version:  "v1.0.0",
				Revision: "",
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "true if both Version and Revision are not empty",
			args: args{
				Version:  "v1.0.0",
				Revision: "abcde",
			},
			want:    true,
			wantErr: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			Version = tt.args.Version
			Revision = tt.args.Revision
			got := IsDebug()

			if got != tt.want {
				t.Errorf("got = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func Test_GetVersion(t *testing.T) {
	type args struct {
		Version  string
		Revision string
	}

	cases := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// NOTE: The behavior of `debug.ReadBuildInfo` has changed since go1.24,
		// these tests get `got = "(devel)", want ""`
		// so the following cases are commented out
		// TODO: Uncomment when fixed
		// See: https://github.com/go-to-k/cls3/issues/364
		// {
		// 	name: "Both Version and Revision are empty",
		// 	args: args{
		// 		Version:  "",
		// 		Revision: "",
		// 	},
		// 	want:    "",
		// 	wantErr: false,
		// },
		// {
		// 	name: "Version is empty and Revision is not empty",
		// 	args: args{
		// 		Version:  "",
		// 		Revision: "abcde",
		// 	},
		// 	want:    "",
		// 	wantErr: false,
		// },
		{
			name: "Revision is not empty and Revision is empty",
			args: args{
				Version:  "v1.0.0",
				Revision: "",
			},
			want:    "v1.0.0",
			wantErr: false,
		},
		{
			name: "Both Version and Revision are not empty",
			args: args{
				Version:  "v1.0.0",
				Revision: "abcde",
			},
			want:    "v1.0.0-abcde",
			wantErr: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			Version = tt.args.Version
			Revision = tt.args.Revision
			got := GetVersion()

			if got != tt.want {
				t.Errorf("got = %#v, want %#v", got, tt.want)
			}
		})
	}
}
