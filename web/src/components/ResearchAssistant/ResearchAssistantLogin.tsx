import { Box, Button, Form, Heading, TextInput } from "@nypl/design-system-react-components";
import React, { useState } from "react";

const Login: React.FC = () => {
    const [username, setUsername] = useState("");
    const [password, setPassword] = useState("");
    const [error, setError] = useState("");
    const [isLoading, setIsLoading] = useState(false);

    const handleLogin = async (e: React.FormEvent) => {
        e.preventDefault();
        setIsLoading(true);
        setError("");
        // this does not validate the user, just encodes and stores the token
        try {
            const token = Buffer.from(`${username}:${password}`).toString("base64");
            localStorage.setItem("authToken", token);
            window.location.href = "/research-assistant-landing";
        } catch (err) {
            setError("Login failed. Please check your credentials.");
            console.error(err);
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <Box
            style={{
                maxWidth: 400,
                margin: "80px auto",
                padding: 32,
                background: "#fff",
                borderRadius: 8,
            }}
        >
            <Heading>Login</Heading>
            <Form onSubmit={handleLogin}>
                <TextInput
                    labelText="Username"
                    onChange={(e) => setUsername(e.target.value)}
                    type="text"
                    value={username}
                    required
                />
                <TextInput
                    labelText="Password"
                    onChange={(e) => setPassword(e.target.value)}
                    type="password"
                    value={password}
                    required
                />
                <Button variant="primary" type="submit" isDisabled={isLoading}>
                    {isLoading ? "Logging in..." : "Login"}
                </Button>
                {error && <Box style={{ color: "red" }}>{error}</Box>}
            </Form>
        </Box>
    );
};

export default Login;
