package connection

import "testing"

func TestConfig_String(t *testing.T) {
	type fields struct {
		Username string
		Password string
		Host     string
		Port     string
		Path     string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "should create connection string successfully",
			fields: fields{
				Username: "guest",
				Password: "guest",
				Host:     "localhost",
				Port:     "1234",
				Path:     "/test",
			},
			want: "amqp://guest:guest@localhost:1234/test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				Username: tt.fields.Username,
				Password: tt.fields.Password,
				Host:     tt.fields.Host,
				Port:     tt.fields.Port,
				Path:     tt.fields.Path,
			}
			if got := c.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
