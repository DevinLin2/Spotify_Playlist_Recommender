import { useEffect, useState } from "react";
import { useSession, signIn, signOut } from 'next-auth/react';
import Button from 'react-bootstrap/Button';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Navbar from 'react-bootstrap/Navbar';
import Nav from 'react-bootstrap/Nav';
import Container from 'react-bootstrap/Container';
import Card from 'react-bootstrap/Card';
import Form from 'react-bootstrap/Form';
import Head from 'next/head'

export default function Home() {

  const [userInput, setUserInput] = useState("");
  const { data: session } = useSession();
  const [showResult, setShowResult] = useState(false);
  const [results, setResults] = useState([]);

  async function handleSubmit(event) {
    event.preventDefault();
    const playlistResponse = await fetch(`http://localhost:8000/square/` + userInput);
    const playlistProps = await playlistResponse.json();
    setResults(playlistProps);
    setResults([
      { name: "playlist1", tracks: ["song1", "song2", "song3"] },
      { name: "playlist2", tracks: ["song4", "song5", "song6"] },
      { name: "playlist3", tracks: ["song4", "song5", "song6"] },
      { name: "playlist4", tracks: ["song4", "song5", "song6"] },
      { name: "playlist5", tracks: ["song4", "song5", "song6"] },
      { name: "playlist6", tracks: ["song4", "song5", "song6"] },
      { name: "playlist7", tracks: ["song4", "song5", "song6"] },
      { name: "playlist8", tracks: ["song4", "song5", "song6"] },
      { name: "playlist9", tracks: ["song4", "song5", "song6"] },
      { name: "playlist10", tracks: ["song4", "song5", "song6"] }
    ])
    setShowResult(true);
  }

  // useEffect(() => {
  //   if (showResult) {
  //     console.log(results);
  //   }
  // }, [results]);

  return (
    <div>
      <Head>
        <title>Spotify Playlist Recommender</title>
        <meta name="viewport" content="initial-scale=1.0, width=device-width" />
      </Head>
      <Navbar bg="dark" variant="dark">
        <Container fluid>
          <Navbar.Brand>
            Spotify Playlist Recommender
          </Navbar.Brand>
          {session && <Button onClick={() => signOut()}>Sign Out</Button>}
          {!session && <Button onClick={() => signIn()}>Sign In</Button>}
        </Container>
      </Navbar>
      <br></br>
      <Form onSubmit={handleSubmit}>
        <Container>
          <Row>
            <Col>
              <Form.Group className="mb-3" controlId="title">
                <Form.Label>Enter playlist preferences:</Form.Label>
                <Form.Control type="text" placeholder="Enter preferences..." value={userInput} onChange={(e) => setUserInput(e.target.value)} />
              </Form.Group>
            </Col>
          </Row>
          <Row>
            <Col>
              <Button className="float-end" variant="primary" type="submit">Get Playlists</Button>
            </Col>
          </Row>
          {showResult && <div>
            <Row>
              <Col>Results:</Col>
            </Row>
            <Row xs={1} md={2} className="g-4">
              {Array.from({ length: 10 }).map((_, idx) => (
                <Col>
                  <Card>
                    <Card.Img variant="top" src="holder.js/100px160" />
                    <Card.Body>
                      <Card.Title>{results[idx].name}</Card.Title>
                      <Card.Text>
                        {results[idx].tracks[0]}<br />
                        {results[idx].tracks[1]}<br />
                        {results[idx].tracks[2]}<br />
                      </Card.Text>
                    </Card.Body>
                  </Card>
                </Col>
              ))}
            </Row>
            <br />
            <Row>
              <Col>
                How were our results?
              </Col>
            </Row>
            <Row>
              <Col xs={6}>
                <Button variant="success">Excellent</Button>{' '}
                <Button variant="warning">Mediocre</Button>{' '}
                <Button variant="danger">Terrible</Button>{' '}
              </Col>
            </Row>
            <br />
          </div>}
        </Container>
      </Form>
    </div >
  )
}